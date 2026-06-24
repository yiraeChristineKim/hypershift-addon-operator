# Managing HostedClusters from the Hub with `hcp from-hub`

`hcp from-hub` lets you create, edit, and delete HostedClusters on a hosting
ManagedCluster **from the hub**, without needing direct access to the hosting
cluster's kubeconfig.

All requests flow through the `hypershift-addon-operator` HCP proxy
(`hcp.ocm.io/v1alpha1` extension API), which forwards them to the hosting
cluster via cluster-proxy.

```
hcp CLI → hub kube-apiserver → HCP proxy → cluster-proxy → hosting cluster
```

---

## Prerequisites


| Requirement           | Notes                                                    |
| --------------------- | -------------------------------------------------------- |
| ACM / MCE hub cluster | The `hypershift-addon-operator` must be running          |
| `cluster-proxy` addon | Enabled on the hosting ManagedCluster                    |
| Hub kubeconfig        | `$KUBECONFIG`, `~/.kube/config`, or `--hub-kubeconfig`   |
| RBAC                  | `managedcluster:admin` permission on the hosting cluster |


---

## Shared flags

These flags are available on every `hcp from-hub` subcommand:


| Flag                | Default                          | Description                                                                                        |                           |     |     |
| ------------------- | -------------------------------- | -------------------------------------------------------------------------------------------------- | ------------------------- | --- | --- |
| `--hub-kubeconfig`  | `$KUBECONFIG` / `~/.kube/config` | Path to the hub cluster kubeconfig                                                                 |                           |     |     |
| `--hosting-cluster` | *(required)*                     | Name of the hosting ManagedCluster                                                                 |                           |     |     |
| `--namespace`       | `clusters`                       | Namespace for HostedCluster resources                                                              |                           |     |     |
|                     | `--context`                      | *(current context)*                                                                                | Kubeconfig context to use |     |     |
| `--proxy-url`       | *(empty)*                        | Connect directly to the HCP proxy for local testing. Skips hub auth and disables TLS verification. |                           |     |     |


---

## Create

`hcp from-hub create` renders resources with the standard `hcp create cluster`
logic and applies them to the hosting cluster through the HCP proxy.

### Platform subcommands

```
hcp from-hub create <platform> [flags]
```

Supported platforms: `aws`, `azure`, `agent`, `kubevirt`, `openstack`

Each platform subcommand accepts the **same flags** as the corresponding
`hcp create cluster <platform>` command.

### How it works internally

1. Runs `hcp create cluster <platform>` in render mode (`--render --render-sensitive`) to produce YAML.
2. Parses the YAML to extract `HostedCluster`, `NodePool(s)`, and `Secret` documents.
3. Stamps all resources with identifying labels (see [Resource labels](#resource-labels)).
4. POSTs a `CreateRequest` to the HCP proxy, which creates the resources on the hosting cluster in dependency order:
  `Namespace → Secrets → HostedCluster → NodePool(s)`

### Examples

**AWS**

```bash
hcp from-hub create aws \
  --hosting-cluster local-cluster \
  --name my-cluster \
  --release-image quay.io/openshift-release-dev/ocp-release:4.17.0-x86_64 \
  --pull-secret ./pull-secret.json \
  --base-domain example.com \
  --aws-creds ~/.aws/credentials \
  --region us-east-1 \
  --generate-ssh
```

**Azure**

```bash
hcp from-hub create azure \
  --hosting-cluster local-cluster \
  --name my-cluster \
  --release-image quay.io/openshift-release-dev/ocp-release:4.17.0-x86_64 \
  --pull-secret ./pull-secret.json \
  --azure-creds ./azure-creds.json \
  --location eastus \
  --base-domain example.com
```

**Agent**

```bash
hcp from-hub create agent \
  --hosting-cluster local-cluster \
  --name my-cluster \
  --release-image quay.io/openshift-release-dev/ocp-release:4.17.0-x86_64 \
  --pull-secret ./pull-secret.json \
  --agent-namespace hardware-provisioning \
  --base-domain example.com
```

---

## Edit

`hcp from-hub edit` works like `kubectl edit`: it fetches the live
`HostedCluster`, opens it in your editor, and applies a strategic merge patch
when you save.

```bash
hcp from-hub edit <name> --hosting-cluster <cluster>
```

### Editor selection

The editor is resolved in this order:

1. `$VISUAL`
2. `$EDITOR`
3. `vi` (Linux / macOS) or `notepad` (Windows)

Editors with arguments are supported (e.g. `VISUAL="code --wait"`).

### Edit loop behaviour


| Situation            | What happens                                                        |
| -------------------- | ------------------------------------------------------------------- |
| File saved unchanged | Exits: `Edit cancelled, no changes made.`                           |
| Invalid YAML saved   | Error is shown; editor re-opens with your edits                     |
| Server rejects PATCH | Error is prepended in a comment; editor re-opens                    |
| Valid change saved   | Strategic merge patch applied; prints `hostedcluster/<name> edited` |


### Example

```bash
# Uses $VISUAL or $EDITOR; falls back to vi
hcp from-hub edit my-cluster --hosting-cluster local-cluster

# Explicit editor
EDITOR=nano hcp from-hub edit my-cluster --hosting-cluster local-cluster
```

---

## Delete

```bash
hcp from-hub delete <name> --hosting-cluster <cluster>
```

Sends a `DELETE` request to the HCP proxy which triggers deletion of the
`HostedCluster` on the hosting cluster.

### Example

```bash
hcp from-hub delete my-cluster --hosting-cluster local-cluster
```

---

## Resource labels

Every resource created through `hcp from-hub create` carries these labels on
the hosting cluster:


| Label                      | Value          | Set by             |
| -------------------------- | -------------- | ------------------ |
| `hcp.ocm.io/created-via`   | `hcp-from-hub` | HCP proxy (server) |
| `hcp.ocm.io/created-by`    | `from-hub-cli` | `hcp` CLI (client) |
| `hcp.ocm.io/hostedcluster` | `<name>`       | both               |


This lets you find all resources belonging to a cluster:

```bash
kubectl get secrets,hostedclusters,nodepools \
  -l hcp.ocm.io/hostedcluster=my-cluster -A
```

---

## Authentication and authorization

### Production (hub cluster with ACM/MCE)

Your hub kubeconfig credentials are used to authenticate against the hub
kube-apiserver. The kube-apiserver injects your identity as
`X-Remote-User` / `X-Remote-Group` headers before forwarding to the HCP proxy.

The proxy:

1. Checks that you hold `managedcluster:admin` on the target hosting cluster
  via the `clusterview.open-cluster-management.io` API.
2. Impersonates your identity (`Impersonate-User`, `Impersonate-Group`) toward
  cluster-proxy so the hosting cluster enforces its own RBAC for your user.

### Local development (kind / non-ACM)

When `clusterview.open-cluster-management.io` is not installed (e.g. kind),
the permission check is skipped non-fatally and any authenticated user can call
the proxy.

---

## Local development

Use `--proxy-url` to bypass the hub kube-apiserver and talk directly to the
HCP proxy. This is useful when the proxy is exposed via `kubectl port-forward`
or run as a local binary.

```bash
# 1. Port-forward the proxy from a kind cluster
kubectl port-forward -n multicluster-engine \
  svc/hypershift-addon-hcp-proxy 8443:443

# 2. Run commands against it directly
hcp from-hub create agent \
  --proxy-url https://localhost:8443 \
  --hosting-cluster local-cluster \
  --name my-cluster \
  --pull-secret ./pull-secret.json \
  --agent-namespace hardware-provisioning

hcp from-hub create aws \
  --proxy-url https://localhost:8443 \
  --hosting-cluster local-cluster \
  --name my-cluster \
  --release-image quay.io/openshift-release-dev/ocp-release:4.17.0-x86_64 \
  --pull-secret ./pull-secret.json \
  --base-domain example.com \
  --aws-creds ~/.aws/credentials \
  --region us-east-1 \
  --generate-ssh

hcp from-hub edit my-cluster \
  --proxy-url https://localhost:8443 \
  --hosting-cluster local-cluster

hcp from-hub delete my-cluster \
  --proxy-url https://localhost:8443 \
  --hosting-cluster local-cluster
```

!!! note
    `--proxy-url` skips hub kube-apiserver authentication and disables TLS
    verification (the proxy uses a self-signed certificate in local
    environments). Do not use this flag in production.