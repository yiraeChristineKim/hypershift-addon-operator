package manager

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ghodss/yaml"
	"github.com/go-logr/logr"
	configv1 "github.com/openshift/api/config/v1"
	tlspkg "github.com/openshift/controller-runtime-common/pkg/tls"
	hypershiftv1beta1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	libgocrypto "github.com/openshift/library-go/pkg/crypto"
	mcev1 "github.com/stolostron/backplane-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	hcpProxyServiceName = "hypershift-addon-hcp-proxy"
	// Port 9443 avoids conflict with library-go's controllercmd secure-serving
	// which binds :8443 for its own health/metrics endpoint in the same process.
	hcpProxyListenAddr = ":9443"
	hcpProxyAPIGroup    = "hcp.ocm.io"
	hcpProxyAPIVersion  = "v1alpha1"
	hcpProxyResource    = "hostedclusters"
	clusterProxyBaseURL = "https://cluster-proxy-addon-user.open-cluster-management-addon.svc:9092"
	searchGraphQLURL    = "https://search-search-api.open-cluster-management.svc:4010/searchapi/graphql"

	// Mount path for the Secret created by service-ca-operator (OpenShift only).
	hcpProxyTLSDir = "/etc/hcp-proxy/tls"

	// labelCreatedVia is stamped on every resource created through this proxy.
	labelCreatedVia      = "hcp.ocm.io/created-via"
	labelCreatedViaValue = "hcp-from-hub"

	// labelHostedCluster records the owning HostedCluster name on every related resource.
	labelHostedCluster = "hcp.ocm.io/hostedcluster"
)

// Overridable in tests to point at a temp dir.
var (
	certFilePath = hcpProxyTLSDir + "/tls.crt"
	keyFilePath  = hcpProxyTLSDir + "/tls.key"
)

// CreateRequest mirrors the output of `hcp create cluster --render`.
// Pass the complete Kubernetes objects exactly as --render produces them.
// The proxy applies them to the spoke in dependency order:
//
//	Namespace (auto-created, idempotent) → Secrets → HostedCluster → NodePool(s)
//
// Example: run `hcp create cluster agent --render` and convert the YAML to JSON.
type CreateRequest struct {
	// HostedCluster is required. spec.pullSecret.name must reference a Secret
	// in the Secrets list (same as --render output).
	HostedCluster *hypershiftv1beta1.HostedCluster `json:"hostedCluster"`

	// NodePools is the list of NodePools to create (--render may produce more than one).
	NodePools []*hypershiftv1beta1.NodePool `json:"nodePools,omitempty"`

	// Secrets holds every Secret that --render outputs: pull-secret, ssh-key,
	// and (for cloud platforms) any STS/credential secrets.
	// Each Secret is created on the spoke before the HostedCluster.
	Secrets []corev1.Secret `json:"secrets,omitempty"`
}

// ResourceBundle is the response body for GET/POST/PATCH .../hostedclusters/{name}/resources.
// Secrets are never included — the pull-secret field in HostedCluster.Spec is a
// LocalObjectReference (name only), so no sensitive data is exposed.
type ResourceBundle struct {
	Namespace     *corev1.Namespace                `json:"namespace,omitempty"`
	HostedCluster *hypershiftv1beta1.HostedCluster `json:"hostedCluster"`
	NodePools     []hypershiftv1beta1.NodePool     `json:"nodePools,omitempty"`
}


// hcpProxy holds shared state for the proxy HTTP server.
type hcpProxy struct {
	hubConfig         *rest.Config
	hubClient         client.Client
	operatorNamespace string
	clusterProxyURL   string              // defaults to clusterProxyBaseURL; overridable in tests
	profileSpec       configv1.TLSProfileSpec // cluster TLS profile applied to server + outbound clients
	log               logr.Logger
}

// StartHCPProxy starts the HCP proxy HTTPS server on :8443, applies hub-side
// manifests, and serves the hcp.ocm.io/v1alpha1 extension API.
// profileSpec must be pre-fetched by the caller (manager.go) so the same profile
// can be shared with the SecurityProfileWatcher for runtime change detection.
func StartHCPProxy(ctx context.Context, profileSpec configv1.TLSProfileSpec, hubConfig *rest.Config, hubClient client.Client, log logr.Logger) error {
	operatorNamespace := resolveOperatorNamespace(ctx, hubClient, log)

	// Allow local dev to override the cluster-proxy URL via env var so the proxy
	// can reach an in-cluster service via kubectl port-forward.
	// Example: export CLUSTER_PROXY_URL=https://localhost:9092
	clusterProxyURL := clusterProxyBaseURL
	if override := os.Getenv("CLUSTER_PROXY_URL"); override != "" {
		clusterProxyURL = override
		log.Info("cluster-proxy URL overridden by CLUSTER_PROXY_URL env var", "url", clusterProxyURL)
	}

	p := &hcpProxy{
		hubConfig:         hubConfig,
		hubClient:         hubClient,
		operatorNamespace: operatorNamespace,
		clusterProxyURL:   clusterProxyURL,
		profileSpec:       profileSpec,
		log:               log,
	}

	if err := p.applyHCPProxyManifests(ctx); err != nil {
		log.Error(err, "failed to apply HCP proxy manifests (non-fatal, continuing)")
	}

	cert, err := loadOrGenerateCert(operatorNamespace, log)
	if err != nil {
		return fmt.Errorf("failed to load/generate TLS cert: %w", err)
	}

	// Apply the cluster's APIServer TLS profile (MinVersion + CipherSuites) to the server.
	tlsConfigFn, unsupported := tlspkg.NewTLSConfigFromProfile(profileSpec)
	if len(unsupported) > 0 {
		log.Info("TLS profile contains unsupported ciphers, they will be ignored", "ciphers", unsupported)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	tlsConfigFn(tlsCfg)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", p.handleHealthz)
	mux.HandleFunc("/readyz", p.handleHealthz)
	mux.HandleFunc("/apis/"+hcpProxyAPIGroup, p.handleDiscovery)
	mux.HandleFunc("/apis/"+hcpProxyAPIGroup+"/"+hcpProxyAPIVersion, p.handleDiscovery)
	mux.HandleFunc("/apis/"+hcpProxyAPIGroup+"/"+hcpProxyAPIVersion+"/", p.handleRoute)

	server := &http.Server{
		Addr:    hcpProxyListenAddr,
		Handler: mux,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 30 * time.Second,
	}

	log.Info("starting HCP proxy server", "addr", hcpProxyListenAddr)

	errCh := make(chan error, 1)
	go func() {
		if err := server.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutCtx)
	case err := <-errCh:
		return err
	}
}

// resolveOperatorNamespace returns the MCE target namespace (defaults to multicluster-engine).
func resolveOperatorNamespace(ctx context.Context, hubClient client.Client, log logr.Logger) string {
	ns := "multicluster-engine"
	mceList := &mcev1.MultiClusterEngineList{}
	if err := hubClient.List(ctx, mceList); err == nil && len(mceList.Items) > 0 {
		if mceList.Items[0].Spec.TargetNamespace != "" {
			ns = mceList.Items[0].Spec.TargetNamespace
		}
	} else if err != nil {
		log.Error(err, "failed to list MultiClusterEngine, defaulting namespace to multicluster-engine")
	}
	return ns
}

// applyHCPProxyManifests creates/updates the Service, APIService, and RBAC resources.
func (p *hcpProxy) applyHCPProxyManifests(ctx context.Context) error {
	ns := p.operatorNamespace

	// Service
	svcBytes, err := fs.ReadFile("manifests/hcp-proxy/service.yaml")
	if err != nil {
		return fmt.Errorf("read service.yaml: %w", err)
	}
	svc := &corev1.Service{}
	if err := yaml.Unmarshal(svcBytes, svc); err != nil {
		return fmt.Errorf("parse service.yaml: %w", err)
	}
	svc.Namespace = ns
	if err := applyService(ctx, p.hubClient, svc); err != nil {
		return fmt.Errorf("apply service: %w", err)
	}

	// ClusterRole + ClusterRoleBinding (multi-document YAML)
	rbacBytes, err := fs.ReadFile("manifests/hcp-proxy/rbac.yaml")
	if err != nil {
		return fmt.Errorf("read rbac.yaml: %w", err)
	}
	if err := p.applyRBAC(ctx, rbacBytes, ns); err != nil {
		return fmt.Errorf("apply rbac: %w", err)
	}

	// APIService — cluster-scoped, applied via dynamic client with server-side apply
	apiSvcBytes, err := fs.ReadFile("manifests/hcp-proxy/apiservice.yaml")
	if err != nil {
		return fmt.Errorf("read apiservice.yaml: %w", err)
	}
	if err := p.applyAPIService(ctx, apiSvcBytes, ns); err != nil {
		return fmt.Errorf("apply apiservice: %w", err)
	}

	p.log.Info("HCP proxy manifests applied", "namespace", ns)
	return nil
}

func applyService(ctx context.Context, c client.Client, desired *corev1.Service) error {
	existing := &corev1.Service{}
	err := c.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	existing.Spec.Selector = desired.Spec.Selector
	existing.Spec.Ports = desired.Spec.Ports
	return c.Update(ctx, existing)
}

func (p *hcpProxy) applyRBAC(ctx context.Context, rawYAML []byte, ns string) error {
	for _, doc := range splitYAMLDocs(rawYAML) {
		var meta struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal(doc, &meta); err != nil {
			return err
		}

		switch meta.Kind {
		case "ClusterRole":
			cr := &rbacv1.ClusterRole{}
			if err := yaml.Unmarshal(doc, cr); err != nil {
				return err
			}
			existing := &rbacv1.ClusterRole{}
			if err := p.hubClient.Get(ctx, types.NamespacedName{Name: cr.Name}, existing); apierrors.IsNotFound(err) {
				if err := p.hubClient.Create(ctx, cr); err != nil {
					return err
				}
			} else if err != nil {
				return err
			} else {
				existing.Rules = cr.Rules
				if err := p.hubClient.Update(ctx, existing); err != nil {
					return err
				}
			}

		case "ClusterRoleBinding":
			crb := &rbacv1.ClusterRoleBinding{}
			if err := yaml.Unmarshal(doc, crb); err != nil {
				return err
			}
			for i := range crb.Subjects {
				crb.Subjects[i].Namespace = ns
			}
			existing := &rbacv1.ClusterRoleBinding{}
			if err := p.hubClient.Get(ctx, types.NamespacedName{Name: crb.Name}, existing); apierrors.IsNotFound(err) {
				if err := p.hubClient.Create(ctx, crb); err != nil {
					return err
				}
			} else if err != nil {
				return err
			} else {
				existing.Subjects = crb.Subjects
				existing.RoleRef = crb.RoleRef
				if err := p.hubClient.Update(ctx, existing); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (p *hcpProxy) applyAPIService(ctx context.Context, rawYAML []byte, ns string) error {
	apiSvc := &apiregistrationv1.APIService{}
	if err := yaml.Unmarshal(rawYAML, apiSvc); err != nil {
		return fmt.Errorf("parse apiservice: %w", err)
	}
	if apiSvc.Spec.Service != nil {
		apiSvc.Spec.Service.Namespace = ns
	}
	apiSvc.APIVersion = "apiregistration.k8s.io/v1"
	apiSvc.Kind = "APIService"

	dynClient, err := dynamic.NewForConfig(p.hubConfig)
	if err != nil {
		return err
	}
	gvr := schema.GroupVersionResource{
		Group:    "apiregistration.k8s.io",
		Version:  "v1",
		Resource: "apiservices",
	}
	data, err := json.Marshal(apiSvc)
	if err != nil {
		return err
	}
	_, err = dynClient.Resource(gvr).Patch(ctx, apiSvc.Name,
		types.ApplyPatchType, data,
		metav1.PatchOptions{FieldManager: "hypershift-addon-manager"})
	return err
}

// splitYAMLDocs splits a multi-document YAML byte slice on "---" separators.
func splitYAMLDocs(data []byte) [][]byte {
	var docs [][]byte
	for _, part := range bytes.Split(data, []byte("\n---")) {
		if trimmed := bytes.TrimSpace(part); len(trimmed) > 0 {
			docs = append(docs, trimmed)
		}
	}
	return docs
}

// loadOrGenerateCert loads the serving cert from the service-ca-operator Secret
// mount (OpenShift), or falls back to a self-signed cert (kind / vanilla k8s).
func loadOrGenerateCert(operatorNS string, log logr.Logger) (tls.Certificate, error) {
	if _, err := os.Stat(certFilePath); err == nil {
		cert, err := tls.LoadX509KeyPair(certFilePath, keyFilePath)
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("load service-ca cert from %s: %w", hcpProxyTLSDir, err)
		}
		log.Info("loaded serving cert from service-ca Secret", "dir", hcpProxyTLSDir)
		return cert, nil
	}
	log.Info("service-ca cert not found, generating self-signed fallback cert", "dir", hcpProxyTLSDir)
	return generateSelfSignedCert(operatorNS)
}

// generateSelfSignedCert creates an ephemeral serving cert via library-go crypto.
// Used only when the service-ca-operator Secret is not available (non-OpenShift).
func generateSelfSignedCert(operatorNS string) (tls.Certificate, error) {
	const certLifetime = 2 * 365 * 24 * time.Hour // within library-go's 7200-day limit

	caConfig, err := libgocrypto.MakeSelfSignedCAConfigForDuration(hcpProxyServiceName+"-ca", certLifetime)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create CA: %w", err)
	}
	ca := &libgocrypto.CA{
		Config:          caConfig,
		SerialGenerator: &libgocrypto.RandomSerialGenerator{},
	}

	hostnames := sets.New[string](
		"localhost",
		"127.0.0.1",
		hcpProxyServiceName,
		hcpProxyServiceName+"."+operatorNS,
		hcpProxyServiceName+"."+operatorNS+".svc",
		hcpProxyServiceName+"."+operatorNS+".svc.cluster.local",
	)
	serverConfig, err := ca.MakeServerCert(hostnames, certLifetime)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create server cert: %w", err)
	}

	certPEM, keyPEM, err := serverConfig.GetPEMBytes()
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("encode server cert PEM: %w", err)
	}

	return tls.X509KeyPair(certPEM, keyPEM)
}

// handleHealthz responds to health/readiness probes.
func (p *hcpProxy) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleDiscovery returns API group / version discovery documents.
func (p *hcpProxy) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if strings.HasSuffix(r.URL.Path, hcpProxyAPIGroup) {
		doc := map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "APIGroup",
			"name":       hcpProxyAPIGroup,
			"versions": []map[string]string{
				{"groupVersion": hcpProxyAPIGroup + "/" + hcpProxyAPIVersion, "version": hcpProxyAPIVersion},
			},
			"preferredVersion": map[string]string{
				"groupVersion": hcpProxyAPIGroup + "/" + hcpProxyAPIVersion,
				"version":      hcpProxyAPIVersion,
			},
		}
		_ = json.NewEncoder(w).Encode(doc)
		return
	}

	// /apis/hcp.ocm.io/v1alpha1
	doc := map[string]interface{}{
		"apiVersion":   "v1",
		"kind":         "APIResourceList",
		"groupVersion": hcpProxyAPIGroup + "/" + hcpProxyAPIVersion,
		"resources": []map[string]interface{}{
			{
				"name":         hcpProxyResource,
				"singularName": "hostedcluster",
				"namespaced":   true,
				"kind":         "HostedCluster",
				"verbs":        []string{"create", "delete", "get", "list"},
			},
		{
			// Alias subresource: same as GET|PATCH /{name} but with an explicit /resources suffix.
			// Both paths return/accept the full ResourceBundle (HostedCluster + NodePools).
			"name":       hcpProxyResource + "/resources",
			"namespaced": true,
			"kind":       "ResourceBundle",
			"verbs":      []string{"get", "update"},
		},
		},
	}
	_ = json.NewEncoder(w).Encode(doc)
}

// handleRoute dispatches all /apis/hcp.ocm.io/v1alpha1/... requests.
func (p *hcpProxy) handleRoute(w http.ResponseWriter, r *http.Request) {
	prefix := "/apis/" + hcpProxyAPIGroup + "/" + hcpProxyAPIVersion + "/"
	remaining := strings.TrimPrefix(r.URL.Path, prefix)
	parts := strings.Split(remaining, "/")

	hostingCluster := r.URL.Query().Get("hostingCluster")

	if hostingCluster == "" {
		http.Error(w, "hostingCluster query parameter is required", http.StatusBadRequest)
		return
	}

	if err := p.checkSpokeHealth(r.Context(), hostingCluster); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	username, groups := whoIsTheCaller(r)
	if err := p.checkHubPermission(r.Context(), username, groups, hostingCluster); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	switch {
	case len(parts) == 3 && parts[0] == "namespaces" && parts[2] == hcpProxyResource:
		ns := parts[1]
		switch r.Method {
		case http.MethodGet:
			p.handleList(w, r, ns, hostingCluster)
		case http.MethodPost:
			p.handleCreate(w, r, ns, hostingCluster)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}

	// GET|PATCH|DELETE .../namespaces/{ns}/hostedclusters/{name}
	// GET also accepts the /resources suffix — both return the full bundle.
	case (len(parts) == 4 || (len(parts) == 5 && parts[4] == "resources")) &&
		parts[0] == "namespaces" && parts[2] == hcpProxyResource:
		ns, name := parts[1], parts[3]
		switch r.Method {
		case http.MethodGet:
			// Returns the full ResourceBundle (HostedCluster + NodePools).
			p.handleGetResources(w, r, ns, name, hostingCluster)
		case http.MethodPatch:
			// Full bundle replace: PUT HostedCluster + all NodePools to the spoke.
			p.handlePatchResources(w, r, ns, name, hostingCluster)
		case http.MethodDelete:
			p.handleDelete(w, r, ns, name, hostingCluster)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}

	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// checkSpokeHealth verifies that the named ManagedCluster is Available.
func (p *hcpProxy) checkSpokeHealth(ctx context.Context, spokeName string) error {
	mc := &clusterv1.ManagedCluster{}
	if err := p.hubClient.Get(ctx, types.NamespacedName{Name: spokeName}, mc); err != nil {
		return fmt.Errorf("managed cluster %q not found: %w", spokeName, err)
	}
	for _, cond := range mc.Status.Conditions {
		if cond.Type == clusterv1.ManagedClusterConditionAvailable {
			if cond.Status == metav1.ConditionTrue {
				return nil
			}
			return fmt.Errorf("managed cluster %q is not available: %s", spokeName, cond.Message)
		}
	}
	return fmt.Errorf("managed cluster %q availability unknown", spokeName)
}

// whoIsTheCaller extracts the authenticated user identity injected by the kube-apiserver.
func whoIsTheCaller(r *http.Request) (username string, groups []string) {
	username = r.Header.Get("X-Remote-User")
	for _, g := range r.Header["X-Remote-Group"] {
		groups = append(groups, strings.Split(g, ",")...)
	}
	return username, groups
}

// checkHubPermission verifies the caller has admin-level access to the hosting cluster
// via the clusterview UserPermission named "managedcluster:admin".
//
// Two-step logic:
//  1. Probe with the operator's own identity (no impersonation) to confirm the
//     clusterview API is installed on this hub. If the API is absent the hub is a
//     dev/kind cluster — skip the check non-fatally so local development still works.
//  2. Re-fetch under the caller's impersonated identity. A 404 at this step means
//     the user does not hold managedcluster:admin on any cluster → hard deny.
//     (View-only callers have a "managedcluster:view" object, not "managedcluster:admin".)
func (p *hcpProxy) checkHubPermission(ctx context.Context, username string, groups []string, hostingCluster string) error {
	if username == "" {
		return fmt.Errorf("unauthenticated request")
	}

	gvr := schema.GroupVersionResource{
		Group:    "clusterview.open-cluster-management.io",
		Version:  "v1alpha1",
		Resource: "userpermissions",
	}

	// Step 1 — probe API availability using the operator's own credentials.
	hubDynClient, err := dynamic.NewForConfig(p.hubConfig)
	if err != nil {
		return fmt.Errorf("failed to create hub dynamic client: %w", err)
	}
	if _, probeErr := hubDynClient.Resource(gvr).Get(ctx, "managedcluster:admin", metav1.GetOptions{}); probeErr != nil {
		if apierrors.IsNotFound(probeErr) &&
			strings.Contains(probeErr.Error(), "the server could not find the requested resource") {
			// API group is not registered (kind / non-ACM hub) — skip non-fatally.
			p.log.Info("clusterview API not installed, skipping hub permission check")
			return nil
		}
		// Any other probe error (network, auth) — skip non-fatally but log it.
		p.log.Error(probeErr, "clusterview probe failed, skipping hub permission check")
		return nil
	}

	// Step 2 — check caller's permissions under impersonation.
	// clusterview API is present; a 404 here means the user is not an admin.
	impConfig := rest.CopyConfig(p.hubConfig)
	impConfig.Impersonate = rest.ImpersonationConfig{
		UserName: username,
		Groups:   groups,
	}
	dynClient, err := dynamic.NewForConfig(impConfig)
	if err != nil {
		return fmt.Errorf("failed to create impersonated client: %w", err)
	}

	item, err := dynClient.Resource(gvr).Get(ctx, "managedcluster:admin", metav1.GetOptions{})
	if err != nil {
		// API exists but the user cannot see this object → not an admin on any cluster.
		return fmt.Errorf("user %q does not have admin access to hosting cluster %q", username, hostingCluster)
	}

	status, ok := item.Object["status"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("user %q does not have admin access to hosting cluster %q", username, hostingCluster)
	}
	bindingList, ok := status["bindings"].([]interface{})
	if !ok {
		return fmt.Errorf("user %q does not have admin access to hosting cluster %q", username, hostingCluster)
	}
	for _, b := range bindingList {
		bMap, ok := b.(map[string]interface{})
		if !ok {
			continue
		}
		if cluster, _ := bMap["cluster"].(string); cluster == hostingCluster {
			return nil
		}
	}
	return fmt.Errorf("user %q does not have admin access to hosting cluster %q", username, hostingCluster)
}

// spokeURL builds the cluster-proxy URL for a resource on the spoke.
func (p *hcpProxy) spokeURL(spokeName, apiPath string) string {
	base := p.clusterProxyURL
	if base == "" {
		base = clusterProxyBaseURL
	}
	return base + "/" + spokeName + apiPath
}

// buildHTTPClient builds an *http.Client using the hub rest.Config for mTLS/auth
// and the cluster TLS profile for MinVersion + CipherSuites. This is the canonical
// way to build outbound HTTP clients so no TLS version is hardcoded.
func (p *hcpProxy) buildHTTPClient(timeout time.Duration) (*http.Client, error) {
	// Build TLS config from rest.Config (CA cert, client cert, server name).
	tlsCfg, err := rest.TLSConfigFor(p.hubConfig)
	if err != nil {
		return nil, fmt.Errorf("TLS config from rest.Config: %w", err)
	}
	// Apply the cluster's OpenShift TLS profile (MinVersion + CipherSuites).
	// No version is hardcoded here — settings come from apiservers.config.openshift.io/cluster.
	tlsConfigFn, _ := tlspkg.NewTLSConfigFromProfile(p.profileSpec)
	tlsConfigFn(tlsCfg)

	// Local dev override: when cluster-proxy is reached via kubectl port-forward the
	// server cert SAN won't match "localhost", so allow skipping TLS verification.
	// Set CLUSTER_PROXY_INSECURE=true only in development — never in production.
	if os.Getenv("CLUSTER_PROXY_INSECURE") == "true" {
		tlsCfg.InsecureSkipVerify = true //nolint:gosec
	}

	base := &http.Transport{
		TLSClientConfig: tlsCfg,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:    100,
		IdleConnTimeout: 90 * time.Second,
	}

	// Wrap base transport with Bearer token / impersonation auth from hub config.
	wrapped, err := rest.HTTPWrappersForConfig(p.hubConfig, base)
	if err != nil {
		return nil, fmt.Errorf("HTTP auth wrappers: %w", err)
	}
	return &http.Client{Transport: wrapped, Timeout: timeout}, nil
}

// spokeHTTPClient builds an http.Client that routes through cluster-proxy
// with Impersonate-User/Group headers for the caller.
func (p *hcpProxy) spokeHTTPClient(username string, groups []string) (*http.Client, error) {
	c, err := p.buildHTTPClient(30 * time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to build spoke client: %w", err)
	}
	c.Transport = &impersonatingTransport{
		wrapped:  c.Transport,
		username: username,
		groups:   groups,
	}
	return c, nil
}

// impersonatingTransport injects Impersonate-User/Group headers on every request.
type impersonatingTransport struct {
	wrapped  http.RoundTripper
	username string
	groups   []string
}

func (t *impersonatingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	if t.username != "" {
		req.Header.Set("Impersonate-User", t.username)
	}
	for _, g := range t.groups {
		req.Header.Add("Impersonate-Group", g)
	}
	return t.wrapped.RoundTrip(req)
}

// handleCreate applies the full set of resources that `hcp create cluster --render`
// produces to the spoke, in the correct dependency order:
//
//	0. Namespace    (auto-created, idempotent — 409 is silently ignored)
//	1. Secrets      (pull-secret, ssh-key, any cloud-provider STS secrets, ...)
//	2. HostedCluster (stamped with labelCreatedVia; spec.pullSecret already set by caller)
//	3. NodePool(s)  (each stamped with labelCreatedVia)
//
// The response is the full ResourceBundle so the caller gets every created object
// in one shot without a follow-up GET /resources round-trip.
func (p *hcpProxy) handleCreate(w http.ResponseWriter, r *http.Request, ns, spokeName string) {
	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.HostedCluster == nil {
		http.Error(w, "hostedCluster is required", http.StatusBadRequest)
		return
	}

	username, groups := whoIsTheCaller(r)
	hcpClient, err := p.spokeHTTPClient(username, groups)
	if err != nil {
		http.Error(w, "failed to build spoke client: "+err.Error(), http.StatusInternalServerError)
		return
	}

	ctx := r.Context()

	hcName := req.HostedCluster.Name

	// addProxyLabels merges the proxy-managed labels into an existing label map.
	addProxyLabels := func(labels map[string]string) map[string]string {
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[labelCreatedVia] = labelCreatedViaValue
		labels[labelHostedCluster] = hcName
		return labels
	}

	// 0. Ensure Namespace (idempotent — 409 means it already exists)
	nsObj := buildNamespace(ns, hcName)
	if err := p.createOnSpoke(ctx, hcpClient, spokeName, ns, "namespaces", nsObj); err != nil && !isAlreadyExists(err) {
		http.Error(w, "failed to ensure namespace: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 1. Create Secrets (pull-secret, ssh-key, cloud-provider STS secrets, …)
	for i := range req.Secrets {
		req.Secrets[i].Namespace = ns
		req.Secrets[i].Labels = addProxyLabels(req.Secrets[i].Labels)
		if err := p.createOnSpoke(ctx, hcpClient, spokeName, ns, "secrets", &req.Secrets[i]); err != nil {
			http.Error(w, "failed to create secret "+req.Secrets[i].Name+": "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// 2. Create HostedCluster
	//    spec.pullSecret.name / spec.sshKey.name are already set by the caller
	//    (same as --render output) — the proxy does NOT construct those names.
	req.HostedCluster.Namespace = ns
	req.HostedCluster.APIVersion = hypershiftv1beta1.GroupVersion.String()
	req.HostedCluster.Kind = "HostedCluster"
	req.HostedCluster.Labels = addProxyLabels(req.HostedCluster.Labels)
	if err := p.createOnSpoke(ctx, hcpClient, spokeName, ns, "hostedclusters", req.HostedCluster); err != nil {
		http.Error(w, "failed to create HostedCluster: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 3. Create NodePool(s)
	var createdNodePools []hypershiftv1beta1.NodePool
	for i := range req.NodePools {
		np := req.NodePools[i]
		if np == nil {
			continue
		}
		np.Namespace = ns
		np.APIVersion = hypershiftv1beta1.GroupVersion.String()
		np.Kind = "NodePool"
		if np.Spec.ClusterName == "" {
			np.Spec.ClusterName = hcName
		}
		np.Labels = addProxyLabels(np.Labels)
		if err := p.createOnSpoke(ctx, hcpClient, spokeName, ns, "nodepools", np); err != nil {
			p.log.Error(err, "failed to create NodePool (non-fatal)", "name", np.Name)
		}
		createdNodePools = append(createdNodePools, *np)
	}

	bundle := &ResourceBundle{
		Namespace:     nsObj,
		HostedCluster: req.HostedCluster,
		NodePools:     createdNodePools,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(bundle)
}

// handleDelete deletes the HostedCluster and all associated NodePools from the spoke.
func (p *hcpProxy) handleDelete(w http.ResponseWriter, r *http.Request, ns, name, spokeName string) {
	username, groups := whoIsTheCaller(r)
	hcpClient, err := p.spokeHTTPClient(username, groups)
	if err != nil {
		http.Error(w, "failed to build spoke client: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// List and delete matching NodePools
	npURL := p.spokeURL(spokeName, "/apis/hypershift.openshift.io/v1beta1/namespaces/"+ns+"/nodepools")
	if resp, err := hcpClient.Get(npURL); err == nil && resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()
		var npList hypershiftv1beta1.NodePoolList
		if jsonErr := json.NewDecoder(resp.Body).Decode(&npList); jsonErr == nil {
			for _, np := range npList.Items {
				if np.Spec.ClusterName == name {
					delURL := p.spokeURL(spokeName, "/apis/hypershift.openshift.io/v1beta1/namespaces/"+ns+"/nodepools/"+np.Name)
					req, _ := http.NewRequestWithContext(r.Context(), http.MethodDelete, delURL, nil)
					_, _ = hcpClient.Do(req)
				}
			}
		}
	}

	// Delete HostedCluster
	delURL := p.spokeURL(spokeName, "/apis/hypershift.openshift.io/v1beta1/namespaces/"+ns+"/hostedclusters/"+name)
	delReq, err := http.NewRequestWithContext(r.Context(), http.MethodDelete, delURL, nil)
	if err != nil {
		http.Error(w, "failed to build delete request: "+err.Error(), http.StatusInternalServerError)
		return
	}
	resp, err := hcpClient.Do(delReq)
	if err != nil {
		http.Error(w, "spoke request failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}


// handlePatchResources works like kubectl edit: accept a full ResourceBundle,
// PUT each resource back to the spoke (full replace), and return the live bundle.
//
// Workflow mirrors kubectl edit:
//  1. GET .../hostedclusters/{name}/resources  → receive ResourceBundle
//  2. Edit the fields you want to change
//  3. PATCH .../hostedclusters/{name}/resources with the modified ResourceBundle
//
// The proxy sends a PUT for the HostedCluster and a PUT for each NodePool present
// in the bundle (identified by metadata.name). Resources absent from the bundle are
// left untouched. Content-Type must be application/json.
func (p *hcpProxy) handlePatchResources(w http.ResponseWriter, r *http.Request, ns, name, spokeName string) {
	var bundle ResourceBundle
	if err := json.NewDecoder(r.Body).Decode(&bundle); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	username, groups := whoIsTheCaller(r)
	hcpClient, err := p.spokeHTTPClient(username, groups)
	if err != nil {
		http.Error(w, "failed to build spoke client: "+err.Error(), http.StatusInternalServerError)
		return
	}

	ctx := r.Context()

	// PUT HostedCluster (full replace — same as kubectl edit saves)
	if bundle.HostedCluster != nil {
		bundle.HostedCluster.Namespace = ns
		if err := p.putOnSpoke(ctx, hcpClient, spokeName,
			"/apis/hypershift.openshift.io/v1beta1/namespaces/"+ns+"/hostedclusters/"+name,
			bundle.HostedCluster); err != nil {
			http.Error(w, "HostedCluster update failed: "+err.Error(), http.StatusBadGateway)
			return
		}
	}

	// PUT each NodePool present in the bundle (identified by metadata.name)
	for i := range bundle.NodePools {
		np := &bundle.NodePools[i]
		if np.Name == "" {
			continue
		}
		np.Namespace = ns
		if err := p.putOnSpoke(ctx, hcpClient, spokeName,
			"/apis/hypershift.openshift.io/v1beta1/namespaces/"+ns+"/nodepools/"+np.Name,
			np); err != nil {
			http.Error(w, fmt.Sprintf("NodePool %q update failed: %s", np.Name, err.Error()), http.StatusBadGateway)
			return
		}
	}

	// Re-fetch the full bundle so the response reflects the live server state.
	p.handleGetResources(w, r, ns, name, spokeName)
}

// putOnSpoke sends a PUT request (full replace) to the spoke kube-apiserver.
func (p *hcpProxy) putOnSpoke(ctx context.Context, httpClient *http.Client, spokeName, apiPath string, obj interface{}) error {
	body, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, p.spokeURL(spokeName, apiPath), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("PUT %s: %w", apiPath, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("spoke returned %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}


// handleList queries ACM Search GraphQL for HostedClusters across hosting clusters.
func (p *hcpProxy) handleList(w http.ResponseWriter, r *http.Request, ns, spokeName string) {
	query := `{"query":"query { searchResult: search(input: [{filters: [{property: \"kind\", values: [\"HostedCluster\"]}]}]) { items } }"}`

	searchClient, err := p.buildHTTPClient(15 * time.Second)
	if err != nil {
		p.log.Error(err, "failed to build search client, returning empty list")
		writeEmptyList(w)
		return
	}

	resp, err := searchClient.Post(searchGraphQLURL, "application/json", strings.NewReader(query))
	if err != nil {
		p.log.Error(err, "ACM Search query failed, returning empty list")
		writeEmptyList(w)
		return
	}
	defer resp.Body.Close()

	var searchResp struct {
		Data struct {
			SearchResult []struct {
				Items []map[string]interface{} `json:"items"`
			} `json:"searchResult"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
		p.log.Error(err, "failed to parse Search response, returning empty list")
		writeEmptyList(w)
		return
	}

	var hcList hypershiftv1beta1.HostedClusterList
	hcList.APIVersion = hcpProxyAPIGroup + "/" + hcpProxyAPIVersion
	hcList.Kind = "HostedClusterList"

	for _, result := range searchResp.Data.SearchResult {
		for _, item := range result.Items {
			itemNS, _ := item["namespace"].(string)
			itemCluster, _ := item["cluster"].(string)
			if ns != "" && itemNS != ns {
				continue
			}
			if spokeName != "" && itemCluster != spokeName {
				continue
			}
			name, _ := item["name"].(string)
			hc := hypershiftv1beta1.HostedCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: itemNS,
					Annotations: map[string]string{
						"hcp.ocm.io/hosting-cluster": itemCluster,
					},
				},
			}
			hcList.Items = append(hcList.Items, hc)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(hcList)
}

// handleGetResources returns all K8s resources that make up a HostedCluster:
//   - Namespace (best-effort — omitted if unreachable)
//   - HostedCluster (pull-secret is a reference only; no Secret data is exposed)
//   - NodePools whose spec.clusterName matches the requested HostedCluster
//
// Resources created via this proxy carry the label hcp.ocm.io/created-via=hcp-from-hub.
func (p *hcpProxy) handleGetResources(w http.ResponseWriter, r *http.Request, ns, name, spokeName string) {
	username, groups := whoIsTheCaller(r)
	hcpClient, err := p.spokeHTTPClient(username, groups)
	if err != nil {
		http.Error(w, "failed to build spoke client: "+err.Error(), http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	bundle := &ResourceBundle{}

	// 1. Namespace (best-effort)
	nsReq, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		p.spokeURL(spokeName, "/api/v1/namespaces/"+ns), nil)
	if nsResp, doErr := hcpClient.Do(nsReq); doErr == nil {
		defer nsResp.Body.Close()
		if nsResp.StatusCode == http.StatusOK {
			var namespace corev1.Namespace
			if json.NewDecoder(nsResp.Body).Decode(&namespace) == nil {
				bundle.Namespace = &namespace
			}
		}
	}

	// 2. HostedCluster (required)
	hcReq, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		p.spokeURL(spokeName, "/apis/hypershift.openshift.io/v1beta1/namespaces/"+ns+"/hostedclusters/"+name), nil)
	hcResp, err := hcpClient.Do(hcReq)
	if err != nil {
		http.Error(w, "spoke request failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer hcResp.Body.Close()
	switch hcResp.StatusCode {
	case http.StatusNotFound:
		http.Error(w, "HostedCluster not found", http.StatusNotFound)
		return
	case http.StatusOK:
		// ok
	default:
		body, _ := io.ReadAll(hcResp.Body)
		http.Error(w, fmt.Sprintf("spoke returned %d: %s", hcResp.StatusCode, string(body)), http.StatusBadGateway)
		return
	}
	var hc hypershiftv1beta1.HostedCluster
	if err := json.NewDecoder(hcResp.Body).Decode(&hc); err != nil {
		http.Error(w, "failed to decode HostedCluster: "+err.Error(), http.StatusInternalServerError)
		return
	}
	bundle.HostedCluster = &hc

	// 3. NodePools — list all in the namespace, keep those belonging to this HC
	npReq, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		p.spokeURL(spokeName, "/apis/hypershift.openshift.io/v1beta1/namespaces/"+ns+"/nodepools"), nil)
	if npResp, doErr := hcpClient.Do(npReq); doErr == nil {
		defer npResp.Body.Close()
		if npResp.StatusCode == http.StatusOK {
			var npList hypershiftv1beta1.NodePoolList
			if json.NewDecoder(npResp.Body).Decode(&npList) == nil {
				for _, np := range npList.Items {
					if np.Spec.ClusterName == name {
						bundle.NodePools = append(bundle.NodePools, np)
					}
				}
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(bundle)
}

func writeEmptyList(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	list := hypershiftv1beta1.HostedClusterList{}
	list.APIVersion = hcpProxyAPIGroup + "/" + hcpProxyAPIVersion
	list.Kind = "HostedClusterList"
	_ = json.NewEncoder(w).Encode(list)
}

// buildNamespace constructs a Namespace stamped with the created-via label.
func buildNamespace(name, hcName string) *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				labelCreatedVia:    labelCreatedViaValue,
				labelHostedCluster: hcName,
			},
		},
	}
}

// buildSecret constructs a Kubernetes Secret.
func buildSecret(name, namespace string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data:       data,
	}
}

// isAlreadyExists reports whether a createOnSpoke error means the resource
// already exists on the spoke (HTTP 409 Conflict).
func isAlreadyExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "spoke returned 409")
}

// createOnSpoke POSTs an object to the spoke kube-apiserver via cluster-proxy.
func (p *hcpProxy) createOnSpoke(ctx context.Context, httpClient *http.Client, spokeName, ns, resource string, obj interface{}) error {
	var apiPath string
	switch resource {
	case "namespaces":
		apiPath = "/api/v1/namespaces" // cluster-scoped — no ns prefix
	case "secrets":
		apiPath = "/api/v1/namespaces/" + ns + "/secrets"
	case "hostedclusters":
		apiPath = "/apis/hypershift.openshift.io/v1beta1/namespaces/" + ns + "/hostedclusters"
	case "nodepools":
		apiPath = "/apis/hypershift.openshift.io/v1beta1/namespaces/" + ns + "/nodepools"
	default:
		return fmt.Errorf("unknown resource type: %s", resource)
	}

	body, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", resource, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.spokeURL(spokeName, apiPath), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", resource, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("spoke returned %d for %s: %s", resp.StatusCode, resource, string(respBody))
	}
	return nil
}

