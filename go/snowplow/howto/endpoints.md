---
type: Usage
title: Endpoint — connection Secrets for RESTAction
description: The Endpoint contract — a plain Kubernetes Secret holding connection details and credentials (server-url, auth, TLS, proxy, AWS SigV4) that RESTAction stages reference via endpointRef.
resource: snowplow
tags:
  - snowplow
  - endpoint
  - restaction
  - secret
timestamp: 2026-08-06T00:00:00Z
---

# `Endpoint`

## Overview

> `Endpoint` defines connection details and credentials for accessing an external API or service. 

It is stored as a Kubernetes [`Secret`](https://kubernetes.io/docs/concepts/configuration/secret/) and consumed by [`RESTAction`](restactions.md) to establish secure HTTP or HTTPS connections.

## `Endpoint` keys

| Key | Type | Description | Required |
|--------|------|-------------|-----------|
| `server-url` | `string` | Base URL of the target API or service. | ✅ |
| `proxy-url` | `string` | Optional proxy address used for outbound HTTP traffic. | ❌ | 
| `token` | `string` | Bearer token for authentication. | ❌ |
| `username` | `string` | Username for basic authentication. | ❌ |
| `password` | `string` | Password for basic authentication. | ❌ |
| `certificate-authority-data` | `string (base64)` | Base64-encoded PEM CA certificate used to verify the remote server. | ❌ |
| `client-certificate-data` | `string (base64)` | Base64-encoded PEM client certificate for mutual TLS authentication. | ❌ |
| `client-key-data` | `string (base64)` | Base64-encoded PEM private key associated with the client certificate. | ❌ |
| `debug` | `boolean` | Logs each request and response at `LOG_LEVEL=debug`: method, URL (no userinfo, query values redacted), header names, and the values of `Content-Type`, `Content-Length` and similar descriptive headers only. `Authorization`, cookies and every other header value are `[REDACTED]`; a request body is logged as its length and a keyed digest, a response body as its declared length; client certificates never appear (#1112). | ❌ |
| `insecure` | `boolean` | If `true`, disables TLS certificate verification. | ❌ |
| `aws-access-key` | `string` | AWS access key id — enables AWS SigV4 request signing. | ❌ |
| `aws-secret-key` | `string` | AWS secret access key (SigV4 signing). | ❌ |
| `aws-region` | `string` | AWS region for SigV4 signing. | ❌ |
| `aws-service` | `string` | AWS service name for SigV4 signing. | ❌ |

## Storage in Kubernetes

`Endpoint` information is stored as a [`Secret`](https://kubernetes.io/docs/concepts/configuration/secret/).  

The secret’s `.data` or `.stringData` must contain the fields above with the exact key names.

Example:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: my-endpoint
  namespace: default
stringData:
  server-url: https://api.example.com
  proxy-url: http://proxy.internal:8080
  token: "abc123"
  username: "admin"
  password: "s3cret"
  certificate-authority-data: "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCg=="
  client-certificate-data: "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCg=="
  client-key-data: "LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQo="
  debug: "true"
  insecure: "false"
```

🔎 Note: Certificate and key values must be base64-encoded strings.

## Behavior Summary

- `server-url` is mandatory; a missing value yields the verbatim error `missed required attribute for endpoint: server-url`
- certificates and keys are expected to be base64-encoded, not raw PEM blocks
- when `insecure` is `true`, the TLS client skips server certificate validation
- if both `client-certificate-data` and `client-key-data` are present, mutual TLS is enabled
- if `certificate-authority-data` is provided, it’s added to the root CA pool
- when `proxy-url` is set, outbound requests are routed through it
- boolean values (`debug`, `insecure`) are parsed from strings via `strconv.ParseBool`
- when `aws-access-key`, `aws-secret-key` and `aws-region` are all present, requests are signed with AWS SigV4 for `aws-service`

(All parsing lives in the shared `plumbing` library — `endpoints.FromSecret`
reads the keys above verbatim, and `http/request/transport.go` builds the TLS /
proxy transport from them.)

