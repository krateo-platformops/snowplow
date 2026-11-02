---
type: Usage
title: "Developer Guide: building and running snowplow from source"
description: Build the snowplow image with ko, load it into a local Kind cluster and run the in-repo development deployment (manifests/ + scripts/), then create a test user and exercise RESTActions.
resource: snowplow
tags:
  - snowplow
  - development
  - kind
  - ko
  - build
timestamp: 2026-08-06T00:00:00Z
---

# Developer Guide: Building and Installing `snowplow`

This guide walks you through creating a local Kubernetes cluster with
[kind](https://kind.sigs.k8s.io/), building the `snowplow` image with
[`ko`](https://ko.build/), and running the **in-repo development deployment**.
Everything here mirrors the maintained dev assets in the repo — `scripts/` and
`manifests/` — so the guide and the tooling cannot drift apart:

| Repo asset | What it does |
|---|---|
| `scripts/kind-up.sh` / `scripts/kind-down.sh` | create / delete the Kind cluster |
| `scripts/build.sh` | `ko` build into the Kind-internal registry (`kind.local`) |
| `scripts/jqmodule-to-configmap.sh` | package `testdata/custom-modules.jq` as the `jq-custom-modules` ConfigMap |
| `manifests/deploy.snowplow.yaml` | namespace + ServiceAccount + NodePort Service + Deployment + RBAC |
| `scripts/reboot.sh` | the whole loop above, from scratch, in one command |
| `scripts/test-restactions.sh` | apply the `RESTAction` CRD + sample RESTActions + `devs`-group RBAC from `testdata/` |

All commands below run from the module root (`go/snowplow/` in the monorepo) —
that is where `.ko.yaml`, `crds/`, `manifests/`, `scripts/` and `testdata/`
live.

## 1. Start a local Kind cluster

Create a local Kubernetes cluster using **Kind** (`scripts/kind-up.sh` does
exactly this). The cluster exposes ports `30081` and `30082` on the host for
easy access to `snowplow` services.

```sh {name=kind-up}
kind get kubeconfig >/dev/null 2>&1 || \
cat <<EOF | kind create cluster --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraPortMappings:
  - containerPort: 30081
    hostPort: 30081
    listenAddress: "127.0.0.1"
    protocol: TCP
  - containerPort: 30082
    hostPort: 30082
    listenAddress: "127.0.0.1"
    protocol: TCP
EOF
```

## 2. Create a namespace

The dev deployment is pinned to the `demo-system` namespace (it is hard-coded
throughout `manifests/deploy.snowplow.yaml`).

```sh {name=create-namespace depends=kind-up}
export NAMESPACE="demo-system"
kubectl create namespace ${NAMESPACE} || true
```

## 3. Build the `snowplow` image with `ko`

Use [`ko`](https://ko.build/) to build the `snowplow` image directly into the
**Kind internal registry** (`kind.local`) — this is `scripts/build.sh`:

```sh {name=build depends=kind-up}
KO_DOCKER_REPO=kind.local ko build --base-import-paths .
```

## 4. Create the ConfigMap for custom `jq` modules

`snowplow` loads custom `jq` modules from the path given by
`--jq-modules-path` / `JQ_MODULES_PATH` at runtime. The dev deployment mounts
them from a `jq-custom-modules` ConfigMap; `scripts/jqmodule-to-configmap.sh`
builds it from the in-repo sample modules:

```sh {name=jq-custom-modules depends=create-namespace}
kubectl create configmap jq-custom-modules \
  --from-file=custom.jq=testdata/custom-modules.jq \
  --namespace=${NAMESPACE}
```

## 5. Apply the `RESTAction` CRD

The CRD must exist **before** you create any `RESTAction` (and before granting
RBAC on the resource):

```sh {name=install-restaction-crd depends=kind-up}
kubectl apply -f ./crds/templates.krateo.io_restactions.yaml
```

## 6. Deploy `snowplow`

`manifests/deploy.snowplow.yaml` deploys:

* a `ServiceAccount` (`snowplow`)
* a `Service` exposed on `NodePort` `30081`, targeting the single `http` port `8081`
* a `Deployment` running the `kind.local/snowplow:latest` image you just built,
  with the dev flags:
  `--debug=false --blizzard=false --port=8081 --authn-namespace=demo-system`
  `--url-authn=http://authn.demo-system.svc.cluster.local:8082`
  `--pretty-log=false --jq-modules-path=/jq-modules`
* a narrow read-only `ClusterRole`/`ClusterRoleBinding` for the snowplow
  ServiceAccount, plus a sample `devs`-group role

```sh {name=deploy depends=jq-custom-modules}
kubectl apply -f manifests/deploy.snowplow.yaml
```

Notes on how this dev deployment differs from the production Helm chart
(`helm/snowplow`):

* **Cache is off.** `CACHE_ENABLED` is not set, and the binary treats anything
  but `"true"`/`"1"`/`"yes"` as disabled (`internal/cache/cache.go`). Every
  read goes straight to the apiserver under the caller's own credentials —
  same data, same RBAC, just slower (see
  [operating.md](operating.md)). That is also why the dev ServiceAccount can
  run with a narrow read-only role, while the chart's ClusterRole grants
  cluster-wide `get`/`list`/`watch` + `SubjectAccessReview` create for the
  informer/RBAC-snapshot machinery.
* **No JWT key material, here or in the chart.** snowplow verifies RS256 tokens
  against a JWKS it fetches on the first authenticated request. With no
  `--jwks-url`, the URL is derived from `--url-authn`:
  `http://authn.demo-system.svc.cluster.local:8082/.well-known/jwks.json`.
  [install.md](install.md) §3 serves a quickstart JWKS at exactly that address;
  until something answers there, authenticated requests get `503`.
* No prewarm-seed / `authn` loopback artifacts are deployed — the chart's
  `seedAuthn` machinery needs the `authn` operator and its CRDs.

> `scripts/reboot.sh` runs steps 1-6 (minus the CRD) from scratch:
> kind-down, kind-up, namespace, `ko` build, jq-modules ConfigMap,
> `kubectl apply -f manifests/`.

## 7. Wait until the `snowplow` deployment is ready

```sh {name=wait-for-snowplow depends=deploy}
kubectl wait deployment/snowplow \
  --namespace ${NAMESPACE} \
  --for=condition=available \
  --timeout=90s
```

A quick smoke test (no auth needed on `/health`):

```sh
curl http://127.0.0.1:30081/health
```

## 8. Create a Krateo PlatformOps user

`/call` authenticates with **two** artifacts, both keyed to the same username:

1. an **access token**: an RS256 JWT, carrying `username` and `groups` claims,
   signed with a private key whose public half is in the JWKS snowplow fetches;
2. a **`<user>-clientconfig` Secret** in the `--authn-namespace`
   (`demo-system` here) holding the user's own Kubernetes credentials in the
   [`Endpoint`](endpoints.md) format. Without it every `/call` returns `401`.

Follow steps **3, 4 and 5 of [install.md](install.md)** with
`NAMESPACE=demo-system`. They generate the signing keypair, serve its JWKS as
the `authn` Service on port `8082` (the address `--url-authn` above points at),
mint the token, and create the CSR-signed client-certificate `clientconfig`
Secret. The created user belongs to the `devs` group.

## 9. RBAC for the Krateo PlatformOps user

After creating a new user, you must assign them a minimal set of RBAC
permissions. Since we are testing [RESTActions][restactions], the user needs at
least read access to that resource.
> Write, create, or delete permissions can be granted at the discretion of the cluster administrator.

Moreover, if the [RESTAction][restactions] invokes any internal cluster APIs
(for example, to list other resources), the user must also have the necessary
permissions to access those resources.

For now, we will grant read-only permissions on [RESTActions][restactions].
Since the user created earlier belongs to the _"devs"_ group, we will, for
simplicity, assign these permissions to the entire _"devs"_ group:

```sh
cat <<EOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: restactions-viewer
rules:
- apiGroups:
  - templates.krateo.io
  resources:
  - restactions
  verbs:
  - get
  - list
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: restactions-viewer
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name:  restactions-viewer
subjects:
- kind: Group
  name: devs
  apiGroup: rbac.authorization.k8s.io
EOF
```

> `scripts/test-restactions.sh` applies a broader variant of this
> (`testdata/rbac.yaml` + `testdata/rbac.restactions.yaml`, granting the
> `devs` group full CRUD on restactions) together with the sample
> RESTActions under `testdata/restactions/` — a fast way to get a populated
> playground. `testdata/curl-samples.txt` collects ready-made `/call`, `/list`
> and `/health` invocations.

From here, continue with the [RESTAction walkthroughs](restactions.md):
[list cluster namespaces](restactions/example-cluster-namespaces.md) and
[invoke an external API](restactions/example-external-api.md).

[restactions]: restactions.md
