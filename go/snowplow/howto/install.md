---
type: Usage
title: Installing snowplow on Kind
description: Quickstart that installs the snowplow Helm chart on a disposable Kind cluster, publishes a JWKS and mints a Krateo user (RS256 JWT + clientconfig Secret), and prepares RBAC for the RESTAction walkthroughs.
resource: snowplow
tags:
  - snowplow
  - install
  - kind
  - helm
  - quickstart
timestamp: 2026-11-01T00:00:00Z
---

# Installing `snowplow` on [Kind][kind]

> **Production deploys** use the Helm chart from this repo (`helm/snowplow`,
> published as `oci://ghcr.io/krateo-platformops/charts/snowplow`, image
> `ghcr.io/krateo-platformops/snowplow`), with `CACHE_ENABLED=true` and the
> runtime tuning described in [operating.md](operating.md) — normally via the
> Krateo installer (see [docs/usage.md](../../../docs/usage.md)). This page is a
> single-node **quickstart** for trying snowplow on a disposable [Kind][kind]
> cluster.

If you have any Docker-compatible container runtime installed (including native Docker, Docker Desktop, or OrbStack), you can easily launch a disposable cluster just for this quickstart using [Kind][kind].

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
EOF
```

## 2. Create a namespace

Create a dedicated namespace where Snowplow and its related resources will live.

```sh {name=create-namespace depends=kind-up}
export NAMESPACE="demo-system"
kubectl create namespace ${NAMESPACE}
```

## 3. Create a token-signing keypair and publish its JWKS

snowplow holds **no key material**. It verifies every Krateo access token as an
**RS256** JWT against RSA public keys it fetches from a **JWKS** document
(`main.go`: `jwtutil.NewJWKSKeySource`; the auth middleware calls
`jwtutil.ValidateWithKeySource`). In production that document is authn's
`<URL_AUTHN>/.well-known/jwks.json`, and authn signs tokens with the matching
private key. This quickstart runs no authn, so you play the issuer: you generate an
RSA keypair, keep the private key on your machine to mint tokens (§4), and publish
the public key as a JWKS document inside the cluster, where snowplow can fetch it.

What snowplow does with that document (`jwtutil.JWKSKeySource` in
`github.com/krateo-platformops/plumbing`, wired in `main.go`):

- **Where it comes from.** The chart value `jwt.jwksUrl` (env `JWT_JWKS_URL`). When
  that is empty, the URL is derived from `URL_AUTHN` as
  `<URL_AUTHN>/.well-known/jwks.json`. When both are empty, the process exits at
  startup.
- **When it is fetched.** On the first token validation, not at boot. snowplow
  starts and serves `/health` without an issuer, and recovers on its own once the
  JWKS answers. Concurrent misses share one fetch, bounded by `jwt.requestTimeout`
  (`5s`).
- **Which keys it keeps.** Only entries with `kty: RSA`, a `use` of `sig` (or no
  `use`) and an `alg` of `RS256` (or no `alg`). Other entries are skipped. A document
  with no usable key counts as a failed fetch.
- **How a token picks its key.** By its `kid` header. A token without a `kid` is
  accepted only while the set holds exactly one key. Non-RSA algorithms, HS256
  included, are rejected before any key is looked up.
- **Rotation.** The set is served from memory for `jwt.cacheTTL` (`5m`). A token
  naming a `kid` the cache does not hold triggers a refetch, at most once per
  `jwt.minRefreshInterval` (`30s`), so a new key is picked up without restarting
  snowplow. Each successful fetch **replaces** the whole set: a key the issuer stops
  publishing stops verifying at the next refresh.
- **When the key can't be resolved.** If the JWKS is unreachable, malformed or
  empty, or holds no key for the token's `kid` while the token may be newer than
  the set (issued after the last successful fetch, or with no `iat`), the request
  gets **`503`** (`jwtutil.ErrKeyUnavailable`, mapped by `middleware.UserConfig`
  and `middleware.RefreshAuth`), not `401`: the token may be fine, and the fault is
  on the server side. If a refresh fails while the cache still holds the token's
  `kid`, the cached key keeps serving. A bad signature, a malformed token or an
  expired token gets **`401`**.
- **A retired key.** A token whose `kid` is absent from a set fetched successfully
  **after** the token was issued was signed with a key the issuer no longer
  publishes (retired, for example to revoke its tokens). It gets **`401`** with the
  cause `KeyNotPublished` (`jwtutil.ErrKeyNotPublished`, plumbing v1.36.0), so the
  portal signs the user in again instead of retrying. Until the cached set is
  refreshed the retired key keeps verifying: a revocation lands within
  `jwt.cacheTTL`.

Generate the keypair and build the JWKS document from its public half:

```sh {name=create-signing-key depends=create-namespace}
export JWT_KID=quickstart-1
export JWT_PRIVATE_KEY=/tmp/snowplow-quickstart-jwt.key
openssl genrsa -out ${JWT_PRIVATE_KEY} 2048

python3 - "${JWT_PRIVATE_KEY}" "${JWT_KID}" > /tmp/snowplow-quickstart-jwks.json <<'PYEOF'
import base64, json, re, subprocess, sys
def b64(d): return base64.urlsafe_b64encode(d).rstrip(b"=").decode()
def be(i): return i.to_bytes((i.bit_length() + 7) // 8, "big")
def rsa(*args): return subprocess.run(["openssl", "rsa", "-in", sys.argv[1], "-noout", *args],
                                      capture_output=True, check=True, text=True).stdout
n = int(rsa("-modulus").strip().split("=", 1)[1], 16)
e = int(re.search(r"[Ee]xponent: (\d+)", rsa("-text")).group(1))
print(json.dumps({"keys": [{"kty": "RSA", "use": "sig", "alg": "RS256",
                            "kid": sys.argv[2], "n": b64(be(n)), "e": b64(be(e))}]}))
PYEOF

kubectl create configmap authn-jwks -n ${NAMESPACE} \
  --from-file=jwks.json=/tmp/snowplow-quickstart-jwks.json
```

Serve the document over HTTP at the address authn would have in this namespace,
`http://authn.${NAMESPACE}.svc.cluster.local:8082/.well-known/jwks.json`. A static
file server is enough, because snowplow only sends a `GET` for it:

```sh {name=serve-jwks depends=create-signing-key}
cat <<EOF | kubectl apply -n ${NAMESPACE} -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: authn
spec:
  replicas: 1
  selector:
    matchLabels: {app: quickstart-jwks}
  template:
    metadata:
      labels: {app: quickstart-jwks}
    spec:
      containers:
      - name: jwks
        image: python:3.12-alpine
        command: ["python3", "-m", "http.server", "8082", "--directory", "/srv"]
        ports:
        - containerPort: 8082
        volumeMounts:
        - name: jwks
          mountPath: /srv/.well-known
          readOnly: true
      volumes:
      - name: jwks
        configMap:
          name: authn-jwks
---
apiVersion: v1
kind: Service
metadata:
  name: authn
spec:
  selector: {app: quickstart-jwks}
  ports:
  - port: 8082
    targetPort: 8082
EOF
kubectl rollout status deployment/authn -n ${NAMESPACE} --timeout=120s
```

> **Trying a rotation.** Generate a second keypair under a new `JWT_KID`, add its
> entry to the `keys` array in the ConfigMap next to the first one, and mint tokens
> with the new key: snowplow fetches the new `kid` the first time a token names it.
> Remove the old entry only once no live token uses it; after the next refresh,
> tokens signed with the removed key get `503`.

## 4. Create a Krateo PlatformOps User

A Krateo access token is an **RS256** JWT whose `kid` header names a key in the JWKS
above, carrying `username` and `groups` claims. The shape is defined by the shared
auth library (`github.com/krateo-platformops/plumbing` `jwtutil`: `CreateToken`
signs, `ValidateWithKeySource` verifies). You can mint one with Python's standard
library and the `openssl` CLI; RS256 is RSASSA-PKCS1-v1_5 over SHA-256, which is
exactly what `openssl dgst -sha256 -sign` produces:

```sh {name=create-krateo-user depends=create-signing-key}
export KRATEO_USER=cyberjoker
export KRATEO_ACCESS_TOKEN=$(python3 - "${JWT_PRIVATE_KEY}" "${JWT_KID}" "${KRATEO_USER}" <<'PYEOF'
import base64, json, subprocess, sys, time
def b64(d): return base64.urlsafe_b64encode(d).rstrip(b"=")
key_file, kid, user = sys.argv[1:4]
now = int(time.time())
header = b64(json.dumps({"alg": "RS256", "typ": "JWT", "kid": kid}).encode())
payload = b64(json.dumps({
    "username": user, "groups": ["devs"],
    "iss": "krateo.io", "sub": user,
    "iat": now, "nbf": now, "exp": now + 8 * 3600,
}).encode())
signing_input = header + b"." + payload
sig = subprocess.run(["openssl", "dgst", "-sha256", "-sign", key_file],
                     input=signing_input, capture_output=True, check=True).stdout
print((signing_input + b"." + b64(sig)).decode())
PYEOF
)

echo "KRATEO_USER=${KRATEO_USER}" > .env
echo "KRATEO_ACCESS_TOKEN=${KRATEO_ACCESS_TOKEN}" >> .env
```

> In production authn mints this token at login with its own private key, and
> publishes the public half at its JWKS endpoint. The inline mint above keeps this
> quickstart free of extra tooling (the `krateoctl add-user` CLI that
> [ADR 0001](../docs/adr/0001-decouple-authn.md) originally relied on is no longer
> published). The user belongs to the `devs` group, which the RBAC below targets.

## 5. Create the user's `clientconfig` Secret

The JWT alone is **not** enough. On every `/call`, after validating the JWT
against the JWKS from §3, snowplow loads the caller's per-user Kubernetes credentials from a
**`<user>-clientconfig` Secret** in `AUTHN_NAMESPACE` — the chart sets
`AUTHN_NAMESPACE` to the **release namespace** (`helm/snowplow/templates/configmap.yaml`).
If the Secret is missing, `/call` returns **401** (`middleware.UserConfig`:
Secret NotFound → Unauthorized). In production the `authn` service creates this
Secret at login (a CSR-signed client certificate with `CN=<user>` /
`O=<groups>` — the same thing `krateoctl add-user` does via the shared
`signup` library). Here we mint the certificate by hand through the Kubernetes
CSR API:

```sh {name=create-clientconfig depends=create-krateo-user}
openssl genrsa -out /tmp/${KRATEO_USER}.key 2048
openssl req -new -key /tmp/${KRATEO_USER}.key \
  -subj "/CN=${KRATEO_USER}/O=devs" -out /tmp/${KRATEO_USER}.csr

cat <<EOF | kubectl apply -f -
apiVersion: certificates.k8s.io/v1
kind: CertificateSigningRequest
metadata:
  name: ${KRATEO_USER}
spec:
  request: $(base64 < /tmp/${KRATEO_USER}.csr | tr -d '\n')
  signerName: kubernetes.io/kube-apiserver-client
  usages:
  - digital signature
  - key encipherment
  - client auth
EOF

kubectl certificate approve ${KRATEO_USER}
until CRT=$(kubectl get csr ${KRATEO_USER} -o jsonpath='{.status.certificate}') \
  && [ -n "${CRT}" ]; do sleep 1; done
CA=$(kubectl config view --raw --minify \
  -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: ${KRATEO_USER}-clientconfig
  namespace: ${NAMESPACE}
stringData:
  server-url: https://kubernetes.default.svc
  certificate-authority-data: ${CA}
  client-certificate-data: ${CRT}
  client-key-data: $(base64 < /tmp/${KRATEO_USER}.key | tr -d '\n')
EOF
```

> The Secret's key names and base64-encoded certificate values follow the
> [`Endpoint`](endpoints.md) contract. The certificate identity (`CN`/`O`) is
> what the Kubernetes apiserver enforces RBAC against on the cache-off
> fallback path; keep it aligned with the JWT's `username`/`groups` claims so
> cache-on (in-process RBAC over the JWT identity) and cache-off agree.

## 6. RBACs for the Krateo PlatformOps User

After creating a new user, you must assign them a minimal set of RBAC permissions.
In this case, since we are testing [RESTActions][restactions], the user needs at least read access to this resource.
> Write, create, or delete permissions can be granted at the discretion of the cluster administrator.

Moreover, if the [RESTAction][restactions] invokes any internal cluster APIs (for example, to list other resources), the user must also have the necessary permissions to access those resources.

For now, we will grant read-only permissions on [RESTActions][restactions].
Since the user created earlier belongs to the _"devs"_ group, we will, for simplicity, assign these permissions to the entire _"devs"_ group:

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


## 7. Deploy snowplow

First install the CRDs snowplow needs. The `snowplow-crds` chart ships the `RESTAction` CRD;
the `authn-crds` chart ships the `serviceaccount.authn.krateo.io` CRD that the snowplow chart's
prewarm-seed allowlist CR requires at install time (a hard dependency — without it the install
fails fast on the unknown kind; see the `seedAuthn` notes in `helm/snowplow/values.yaml`):

```sh {name=install-crds depends=kind-up}
helm install snowplow-crds oci://ghcr.io/krateo-platformops/charts/snowplow-crds
helm install authn-crds oci://ghcr.io/krateo-platformops/charts/authn-crds
```

> On a bare Kind cluster (no `authn` operator running) the prewarm seed's loopback token
> exchange fails at runtime — that is fine here: the seed is best-effort warmth, not a
> correctness gate, and `/call` serves normally.

Then install `snowplow` itself. The chart serves on the single
`http` port `8081` (there is no separate probe port — see
[operating.md](operating.md)), so the NodePort maps to it. `jwt.jwksUrl` points
snowplow at the JWKS you published in §3. Left empty (the chart default), it would
be derived from `URL_AUTHN` as
`http://authn.krateo-system.svc.cluster.local:8082/.well-known/jwks.json`, where
this cluster runs nothing:

```sh {name=install depends=install-crds}
helm install snowplow oci://ghcr.io/krateo-platformops/charts/snowplow \
  --namespace ${NAMESPACE} \
  --set service.type=NodePort --set service.port=8081 --set service.nodePort=30081 \
  --set jwt.jwksUrl=http://authn.${NAMESPACE}.svc.cluster.local:8082/.well-known/jwks.json \
  --set-string 'env.DEBUG=true' --set-string 'env.CACHE_ENABLED=true'
```

> `env.*` values must be **strings**: `values.schema.json` rejects a plain
> `--set env.DEBUG=true` (Helm parses it as a boolean), hence `--set-string`.

> No key Secret is created or mounted: the only JWT settings are the `jwt.*`
> values, which the chart renders as the `JWT_JWKS_*` entries of the container's
> `env:` array (`helm/snowplow/templates/deployment.yaml`). snowplow logs the URL it
> resolved at startup (`JWT verification keys sourced from authn JWKS`, field
> `jwks_url`). Nothing is fetched until the first authenticated request, so a
> wrong URL first shows up as `503` on `/call`.

> `env.*` values are rendered into the chart's `snowplow` ConfigMap and consumed
> via `envFrom`. `CACHE_ENABLED=true`
> is the **chart default** (`helm/snowplow/values.yaml`) and turns on the
> in-process cache path; set it explicitly to `false` for the transparent
> direct-apiserver fallback. (The binary itself defaults to cache-off when the
> variable is absent — only `"true"`, `"1"` or `"yes"` enable it.)


## 8. Wait until the `snowplow` deployment is ready

Finally, wait for the `snowplow` deployment to become **available**.
This ensures all pods are up and running before proceeding.

```sh {name=wait-for-snowplow depends=install}
kubectl wait deployment/snowplow \
  --namespace ${NAMESPACE} \
  --for=condition=available \
  --timeout=90s
```


You are now ready to move on to the next steps. From here, you can start testing the [RESTActions][restactions] to see how the different use cases work in practice. 

Experiment with creating, updating, and querying resources to get a hands-on understanding of the platform's capabilities.

## Related ADRs

- [ADR 0001 — Decouple `authn` from snowplow for testing and operations](../docs/adr/0001-decouple-authn.md)
  (supersedes the older [how-to form](./decoupling-authn-from-snowplow-for-testing.md))




[kind]: https://kind.sigs.k8s.io/
[restactions]: restactions.md
