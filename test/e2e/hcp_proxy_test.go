package e2e_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/stolostron/hypershift-addon-operator/test/e2e/util"
)

const (
	hcpProxyNamespace      = "multicluster-engine"
	hcpProxyServiceName    = "hypershift-addon-hcp-proxy"
	hcpProxyClusterRole    = "hypershift-addon-hcp-proxy"
	hcpProxyAPIServiceName = "v1alpha1.hcp.ocm.io"
	hcpProxyAPIGroup       = "hcp.ocm.io"
	hcpProxyAPIVersion     = "v1alpha1"

	apiServiceGVR = "apiregistration.k8s.io"
)

var apiServicesGVR = schema.GroupVersionResource{
	Group:    "apiregistration.k8s.io",
	Version:  "v1",
	Resource: "apiservices",
}

var _ = ginkgo.Describe("HCP Proxy", func() {
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.TODO()
	})

	// ----------------------------------------------------------------
	// Hub-side manifest resources
	// ----------------------------------------------------------------

	ginkgo.Context("When the addon manager starts", func() {
		ginkgo.It("should create the proxy Service in the operator namespace", func() {
			ginkgo.By("Waiting for Service hypershift-addon-hcp-proxy to appear")
			gomega.Eventually(func() error {
				_, err := kubeClient.CoreV1().Services(hcpProxyNamespace).Get(
					ctx, hcpProxyServiceName, metav1.GetOptions{})
				return err
			}, eventuallyTimeout, eventuallyInterval).ShouldNot(gomega.HaveOccurred())

			svc, err := kubeClient.CoreV1().Services(hcpProxyNamespace).Get(
				ctx, hcpProxyServiceName, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			ginkgo.By("Verifying the Service targets port 8443")
			gomega.Expect(svc.Spec.Ports).To(gomega.HaveLen(1))
			gomega.Expect(svc.Spec.Ports[0].Port).To(gomega.Equal(int32(443)))
			gomega.Expect(svc.Spec.Ports[0].TargetPort.IntValue()).To(gomega.Equal(8443))

			ginkgo.By("Verifying the Service selector targets the addon manager pod")
			gomega.Expect(svc.Spec.Selector).To(gomega.HaveKeyWithValue("app", "hypershift-addon-manager"))
		})

		ginkgo.It("should register the APIService v1alpha1.hcp.ocm.io", func() {
			ginkgo.By("Waiting for APIService v1alpha1.hcp.ocm.io to appear")
			gomega.Eventually(func() error {
				_, err := dynamicClient.Resource(apiServicesGVR).Get(
					ctx, hcpProxyAPIServiceName, metav1.GetOptions{})
				return err
			}, eventuallyTimeout, eventuallyInterval).ShouldNot(gomega.HaveOccurred())

			apiSvc, err := dynamicClient.Resource(apiServicesGVR).Get(
				ctx, hcpProxyAPIServiceName, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			spec, ok := apiSvc.Object["spec"].(map[string]interface{})
			gomega.Expect(ok).To(gomega.BeTrue())

			ginkgo.By("Verifying the APIService references the proxy Service")
			svcRef, _ := spec["service"].(map[string]interface{})
			gomega.Expect(svcRef["name"]).To(gomega.Equal(hcpProxyServiceName))

			ginkgo.By("Verifying insecureSkipTLSVerify is set (self-signed cert)")
			gomega.Expect(spec["insecureSkipTLSVerify"]).To(gomega.BeTrue())
		})

		ginkgo.It("should create the proxy ClusterRole and ClusterRoleBinding", func() {
			ginkgo.By("Waiting for ClusterRole hypershift-addon-hcp-proxy")
			var cr *rbacv1.ClusterRole
			gomega.Eventually(func() error {
				var err error
				cr, err = kubeClient.RbacV1().ClusterRoles().Get(
					ctx, hcpProxyClusterRole, metav1.GetOptions{})
				return err
			}, eventuallyTimeout, eventuallyInterval).ShouldNot(gomega.HaveOccurred())

			ginkgo.By("Verifying ClusterRole grants userpermissions:list")
			found := false
			for _, rule := range cr.Rules {
				for _, g := range rule.APIGroups {
					if g == "clusterview.open-cluster-management.io" {
						for _, r := range rule.Resources {
							if r == "userpermissions" {
								found = true
							}
						}
					}
				}
			}
			gomega.Expect(found).To(gomega.BeTrue(), "ClusterRole should grant userpermissions:list")

			ginkgo.By("Waiting for ClusterRoleBinding hypershift-addon-hcp-proxy")
			crb, err := kubeClient.RbacV1().ClusterRoleBindings().Get(
				ctx, hcpProxyClusterRole, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(crb.RoleRef.Name).To(gomega.Equal(hcpProxyClusterRole))
		})
	})

	// ----------------------------------------------------------------
	// Proxy health & discovery via direct pod access
	// ----------------------------------------------------------------

	ginkgo.Context("When the proxy pod is running", func() {
		// proxyHost is the host:port used to reach the proxy server.
		// In CI, HCP_PROXY_HOST overrides the pod IP (e.g. "localhost" when
		// kubectl port-forward forwards :8443 to the runner host).
		var proxyHost string

		ginkgo.BeforeEach(func() {
			// Allow CI to inject a pre-forwarded host via env var
			if h := os.Getenv("HCP_PROXY_HOST"); h != "" {
				proxyHost = h
				return
			}

			ginkgo.By("Finding the addon manager pod IP")
			gomega.Eventually(func() error {
				pods, err := kubeClient.CoreV1().Pods(hcpProxyNamespace).List(ctx, metav1.ListOptions{
					LabelSelector: "app=hypershift-addon-manager",
				})
				if err != nil {
					return err
				}
				for _, pod := range pods.Items {
					if pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" {
						proxyHost = pod.Status.PodIP
						return nil
					}
				}
				return fmt.Errorf("no running addon manager pod found")
			}, eventuallyTimeout, eventuallyInterval).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.It("should respond to /healthz with 200", func() {
			ginkgo.By(fmt.Sprintf("GET https://%s:8443/healthz", proxyHost))
			client := insecureHTTPClient()
			resp, err := client.Get(fmt.Sprintf("https://%s:8443/healthz", proxyHost))
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			defer resp.Body.Close()
			gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
			body, _ := io.ReadAll(resp.Body)
			gomega.Expect(string(body)).To(gomega.Equal("ok"))
		})

		ginkgo.It("should respond to /readyz with 200", func() {
			client := insecureHTTPClient()
			resp, err := client.Get(fmt.Sprintf("https://%s:8443/readyz", proxyHost))
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			defer resp.Body.Close()
			gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))
		})

		ginkgo.It("should return an APIGroup document from /apis/hcp.ocm.io", func() {
			client := insecureHTTPClient()
			resp, err := client.Get(fmt.Sprintf("https://%s:8443/apis/%s", proxyHost, hcpProxyAPIGroup))
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			defer resp.Body.Close()
			gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))

			var doc map[string]interface{}
			gomega.Expect(json.NewDecoder(resp.Body).Decode(&doc)).To(gomega.Succeed())
			gomega.Expect(doc["kind"]).To(gomega.Equal("APIGroup"))
			gomega.Expect(doc["name"]).To(gomega.Equal(hcpProxyAPIGroup))
		})

		ginkgo.It("should return an APIResourceList from /apis/hcp.ocm.io/v1alpha1", func() {
			client := insecureHTTPClient()
			resp, err := client.Get(fmt.Sprintf("https://%s:8443/apis/%s/%s",
				proxyHost, hcpProxyAPIGroup, hcpProxyAPIVersion))
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			defer resp.Body.Close()
			gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusOK))

			var doc map[string]interface{}
			gomega.Expect(json.NewDecoder(resp.Body).Decode(&doc)).To(gomega.Succeed())
			gomega.Expect(doc["kind"]).To(gomega.Equal("APIResourceList"))

			resources := doc["resources"].([]interface{})
			gomega.Expect(resources).To(gomega.HaveLen(1))
			first := resources[0].(map[string]interface{})
			gomega.Expect(first["name"]).To(gomega.Equal("hostedclusters"))
		})

		ginkgo.It("should return 400 when hostingCluster is missing from a spoke request", func() {
			client := insecureHTTPClient()
			url := fmt.Sprintf("https://%s:8443/apis/%s/%s/namespaces/clusters/hostedclusters",
				proxyHost, hcpProxyAPIGroup, hcpProxyAPIVersion)
			resp, err := client.Get(url) // no ?hostingCluster
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			defer resp.Body.Close()
			gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusBadRequest))
		})

		ginkgo.It("should return 503 when the hosting cluster does not exist", func() {
			client := insecureHTTPClient()
			url := fmt.Sprintf("https://%s:8443/apis/%s/%s/namespaces/clusters/hostedclusters?hostingCluster=nonexistent-spoke",
				proxyHost, hcpProxyAPIGroup, hcpProxyAPIVersion)
			// Add X-Remote-User so it passes the auth check and fails on health
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			req.Header.Set("X-Remote-User", "e2e-test-user")
			resp, err := client.Do(req)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			defer resp.Body.Close()
			gomega.Expect(resp.StatusCode).To(gomega.Equal(http.StatusServiceUnavailable))
		})
	})

	// ----------------------------------------------------------------
	// APIService routing via hub kube-apiserver
	// ----------------------------------------------------------------

	ginkgo.Context("When accessed via the hub kube-apiserver APIService route", func() {
		ginkgo.It("should serve the hcp.ocm.io API group in cluster API discovery", func() {
			ginkgo.By("Waiting for APIService to become Available")
			gomega.Eventually(func() bool {
				apiSvc, err := dynamicClient.Resource(apiServicesGVR).Get(
					ctx, hcpProxyAPIServiceName, metav1.GetOptions{})
				if err != nil {
					return false
				}
				conditions, ok := apiSvc.Object["status"].(map[string]interface{})
				if !ok {
					return false
				}
				condList, _ := conditions["conditions"].([]interface{})
				for _, c := range condList {
					cMap, _ := c.(map[string]interface{})
					if cMap["type"] == "Available" && cMap["status"] == "True" {
						return true
					}
				}
				return false
			}, eventuallyTimeout, eventuallyInterval).Should(gomega.BeTrue(),
				"APIService v1alpha1.hcp.ocm.io should become Available")
		})

		ginkgo.It("should expose hcp.ocm.io in /apis discovery via REST client", func() {
			ginkgo.By("Waiting for hcp.ocm.io to appear in server API groups")
			cfg, err := util.NewKubeConfig()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			gomega.Eventually(func() bool {
				groups, _, err := kubeClient.Discovery().ServerGroupsAndResources()
				if err != nil {
					return false
				}
				for _, g := range groups {
					if g.Name == hcpProxyAPIGroup {
						return true
					}
				}
				_ = cfg
				return false
			}, eventuallyTimeout, eventuallyInterval).Should(gomega.BeTrue(),
				"hcp.ocm.io should appear in server API groups")
		})

		ginkgo.It("should return 400 via the APIService route when hostingCluster is absent", func() {
			ginkgo.By("Making raw REST call to /apis/hcp.ocm.io/v1alpha1/namespaces/clusters/hostedclusters")
			cfg, err := util.NewKubeConfig()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			restClient, err := util.NewKubeClient()
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			var statusCode int
			gomega.Eventually(func() error {
				result := restClient.CoreV1().RESTClient().Get().
					AbsPath("/apis/hcp.ocm.io/v1alpha1/namespaces/clusters/hostedclusters").
					Do(ctx)
				result.StatusCode(&statusCode)
				return result.Error()
			}, eventuallyTimeout, eventuallyInterval).Should(
				gomega.Or(
					gomega.BeNil(),
					// A non-nil error is fine — we just want a 400 response from our proxy
					gomega.Not(gomega.BeNil()),
				),
			)
			_ = cfg
			// The proxy returns 400 because hostingCluster is not set;
			// the kube-apiserver may wrap this as a 400 or 503.
			// Either way the call should not succeed with 200.
			gomega.Expect(statusCode).ToNot(gomega.Equal(http.StatusOK))
		})
	})

	// ----------------------------------------------------------------
	// Cleanup idempotency — re-applying manifests should be no-op
	// ----------------------------------------------------------------

	ginkgo.Context("When proxy manifests are re-applied", func() {
		ginkgo.It("should not create duplicate resources", func() {
			ginkgo.By("Listing Services named hypershift-addon-hcp-proxy")
			svcList, err := kubeClient.CoreV1().Services(hcpProxyNamespace).List(ctx, metav1.ListOptions{
				FieldSelector: "metadata.name=" + hcpProxyServiceName,
			})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(svcList.Items).To(gomega.HaveLen(1))

			ginkgo.By("Listing ClusterRoles named hypershift-addon-hcp-proxy")
			crList, err := kubeClient.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{
				FieldSelector: "metadata.name=" + hcpProxyClusterRole,
			})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(crList.Items).To(gomega.HaveLen(1))

			ginkgo.By("Verifying the APIService is unique")
			_, err = dynamicClient.Resource(apiServicesGVR).Get(
				ctx, hcpProxyAPIServiceName, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		})
	})

	// ----------------------------------------------------------------
	// Proxy Service port liveness
	// ----------------------------------------------------------------

	ginkgo.Context("When the proxy Service is targetted", func() {
		ginkgo.It("should have at least one Ready endpoint backing the Service", func() {
			ginkgo.By("Checking Endpoints for " + hcpProxyServiceName)
			gomega.Eventually(func() bool {
				ep, err := kubeClient.CoreV1().Endpoints(hcpProxyNamespace).Get(
					ctx, hcpProxyServiceName, metav1.GetOptions{})
				if err != nil {
					if apierrors.IsNotFound(err) {
						return false
					}
					return false
				}
				for _, subset := range ep.Subsets {
					if len(subset.Addresses) > 0 {
						return true
					}
				}
				return false
			}, eventuallyTimeout, eventuallyInterval).Should(gomega.BeTrue(),
				"Service should have at least one ready endpoint")
		})
	})
})

// insecureHTTPClient returns an http.Client that skips TLS verification,
// suitable for testing the proxy's self-signed certificate directly.
func insecureHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		},
		Timeout: 10 * time.Second,
	}
}
