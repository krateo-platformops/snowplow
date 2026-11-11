# Frontend integration: write provenance on `/call` (`X-Krateo-Provenance`, #1333)

This guide is for the frontend session that replaces the portal's second write
(the `AuditRecord` CR that `provenance.ts` POSTs after every write) with one
request header on the write itself. Snowplow records the write's audit event
(who, what, where, the outcome, as snowplow saw them) and adds what the header
asserts under `krateo.audit.client.*`. Retiring the `AuditRecord` CR is AUDIT-8
(installer's).

Everything below comes from the code: the decoder
`internal/support/audit/provenance.go`, the emit site in
`internal/handlers/call.go`, the CORS options and the route table in
`main.go`, and the shared audit library
`github.com/krateo-platformops/audit` v0.6.0 (`docs/schema.md`, "Client-asserted
provenance"). The tests named in each section are the proof.

## 1. Gate on the capability

```
GET /capabilities
200 {"capabilities":["call.dryRun","call.fieldValidation","call.provenance","call.raw","call.read.inline"]}
```

Send the header, and stop POSTing the `AuditRecord` CR, only where
`call.provenance` is listed. An older snowplow ignores the header: the write
would run and its provenance would be lost. A `404` from `/capabilities`
means no tokens (see `howto-frontend-call-dryrun-raw.md`, section 1).

## 2. Where to send it

| route | read? |
|---|---|
| `POST`, `PUT`, `PATCH`, `DELETE /call` | **yes**: the write's record (`krateo.action=call`) |
| `POST`, `PUT`, `PATCH /call/dry-run` | **yes**: the dry run's record (`krateo.action=call.dryRun`, `krateo.audit.dry_run=true`) |
| `GET /call`, `POST /call/read`, `/export`, anything else | no: ignored (reads are not audited, and the schema allows client keys on writes only) |

Send it on every `/call` of a confirmed set, the dry runs included. The
header is allowed by snowplow's CORS preflight (`Access-Control-Allow-Headers`
lists `X-Krateo-Provenance`; `TestS1333_CORSPreflightAllowsProvenance`).

## 3. Format

```
X-Krateo-Provenance: v1.<base64url of the JSON, no padding>
```

- `v1.` exactly (lower-case `v`, then `1`, then a dot).
- base64url is RFC 4648 section 5 (`-` and `_`, never `+` or `/`), with no `=`
  padding, no spaces and no line breaks.
- The JSON is one object, UTF-8. Whitespace and key order are free (it is
  JSON); `JSON.stringify` output is what the examples use.

```ts
const b64url = (s: string) =>
  btoa(String.fromCharCode(...new TextEncoder().encode(s)))
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
headers['X-Krateo-Provenance'] = 'v1.' + b64url(JSON.stringify(provenance))
```

Example (370 bytes):

```json
{"source":"portal","agentSessionId":"ses_01-AB","confirmationId":"6f1d2c3b-4a5e-4f60-8b7a-9c0d1e2f3a4b","confirmedSet":{"count":3,"irreversible":true,"sha256":"0a08bb91090b111f15a045b6061bccc4d0b8488d85f99ba57d8632e17061c277"},"op":1,"requestedAt":"2026-11-11T10:20:30.456Z"}
```

```
X-Krateo-Provenance: v1.eyJzb3VyY2UiOiJwb3J0YWwiLCJhZ2VudFNlc3Npb25JZCI6InNlc18wMS1BQiIsImNvbmZpcm1hdGlvbklkIjoiNmYxZDJjM2ItNGE1ZS00ZjYwLThiN2EtOWMwZDFlMmYzYTRiIiwiY29uZmlybWVkU2V0Ijp7ImNvdW50IjozLCJpcnJldmVyc2libGUiOnRydWUsInNoYTI1NiI6IjBhMDhiYjkxMDkwYjExMWYxNWEwNDViNjA2MWJjY2M0ZDBiODQ4OGQ4NWY5OWJhNTdkODYzMmUxNzA2MWMyNzcifSwib3AiOjEsInJlcXVlc3RlZEF0IjoiMjAyNi0xMS0xMVQxMDoyMDozMC40NTZaIn0
```

## 4. The schema (closed)

| JSON key | type | required | rule | recorded as |
|---|---|---|---|---|
| `source` | string | **yes** | `"portal"` | `krateo.audit.client.source` |
| `agentSessionId` | string | no: agent-initiated writes only | 1 to 128 of `[A-Za-z0-9_-]` | `krateo.audit.client.agent_session_id` |
| `confirmationId` | string | no | a UUID, canonical and **lower-case** (`crypto.randomUUID()`) | `krateo.audit.client.confirmation_id` |
| `confirmedSet` | object | with `op` | exactly `{count, irreversible, sha256}`, all three | see below |
| `confirmedSet.count` | integer | yes | at least 1 | `krateo.audit.client.confirmed_set.count` |
| `confirmedSet.irreversible` | boolean | yes | `true` when any op of the set is irreversible | `krateo.audit.client.confirmed_set.irreversible` |
| `confirmedSet.sha256` | string | yes | 64 lower-case hex digits: section 7 | `krateo.audit.client.confirmed_set.sha256` |
| `op` | integer | with `confirmedSet` | at least 0 and less than `confirmedSet.count`: this `/call`'s index in the set | `krateo.audit.client.confirmed_set.op` |
| `requestedAt` | string | no | RFC 3339 with exactly 3 fractional digits, `Z` or `±hh:mm` with hour 00-23 and minute 00-59, a real date and time, and an instant whose UTC form is in the years 0000-9999 (`new Date().toISOString()` of a present-day date always is) | `krateo.audit.client.requested_at`, rewritten in UTC (`...Z`) |

**A write no confirm gate ran for** (a sandbox-confined preview the portal
dispatches without showing the gate) sends `source`, `agentSessionId` when an
agent made it, and `requestedAt`, with **no** `confirmationId`, `confirmedSet`
or `op`. That header is valid. The missing keys are the truthful record that
nothing was confirmed; never send placeholder values. The portal's own
encoder output for this case is a fixture in snowplow
(`internal/support/audit/testdata/provenance_portal_1333/samples.json`,
sample 3, `TestProvenance1333_S_AnUnconfirmedPreviewIsValid`), next to three
confirmed-set samples whose `sha256` is re-checked against their canonical
bytes (`TestProvenance1333_P_ThePortalsEncoderIsRead`).

Any other key is a rejection, and so is:

- a key that differs only in case (`Source`), or a key given twice;
- a value of another JSON type, `null` included (`"op":"1"`, `"irreversible":"true"`);
- an integer written with a fraction or an exponent (`1.0`, `1e1`), or past 64 bits;
- `confirmedSet` without `op`, or `op` without `confirmedSet`;
- a top level that is not an object, or an empty object (no `source`).

**Never taken from the header**, and no key exists for any of them: the user's
identity, the actor's class (human or agent comes from the token's signed
`act` chain), the agent's identity, the verb, the target, the namespace, the
outcome, the status, and the time. Free-text prompts are not carried at all:
an agent write is traced to its prompt through `agentSessionId` in Agentiko's
store. A header that names any of these (`"user"`, `"namespace"`, `"verb"`,
`"outcome"`) is rejected whole.

## 5. Limits

- At most **2048 bytes** (2 KiB) of header value, `v1.` included
  (`MaxProvenanceBytes`, the #1333 contract, not a setting). Because unpadded
  base64 never has a length of 1 mod 4, the longest header that can be valid is
  2047 bytes. A schema-valid header is well under 1 KiB (the example above is
  370), so only an unrelated payload reaches the bound.
- One field line. Two `X-Krateo-Provenance` lines are one value joined with
  `, ` (RFC 9110 section 5.3) and are rejected (`encoding`).

## 6. Rejection

The checks run in this order; the first that fails names the reason:

| reason | when |
|---|---|
| `oversize` | the value is longer than 2048 bytes; nothing is decoded |
| `version` | it does not start with `v1.` (an empty value included) |
| `encoding` | the rest is not unpadded base64url, or what it decodes to is not a UTF-8 JSON text |
| `schema` | the JSON breaks section 4 |

A rejected header **never blocks or changes the write**: the write is sent
to the apiserver exactly as without the header, and the response is the same
bytes. The event then carries `krateo.audit.client.rejected=<reason>` and **no
other `krateo.audit.client.*` key**: a header is accepted or rejected whole,
never in part. Snowplow does not signal the rejection in the response, so
test the encoder against this page (the example above, and section 7's
vectors). `TestProvenance1333_R_*` (one arm per rule), `TestCall1333_R_*`
(through the handler), `FuzzDecodeProvenance` (any value: a closed-set
rejection alone, or an assertion the shared library's `Check` accepts).

No header: no client key, and the record is the one snowplow wrote before
#1333.

## 7. `confirmedSet.sha256`: the canonical confirmed set

The SHA-256, lower-case hex, of the canonical bytes of the confirmed
BlastRadiusSet:

- UTF-8 JSON of the **array of ops in confirmation order**, no whitespace;
- every op has exactly the 7 keys, sorted:
  `{"group","irreversible","name","namespace","resource","verb","version"}`;
- core group `""`; absent name `""`; verb upper-case; booleans `true`/`false`;
- no escaping beyond JSON's minimum (quote, backslash, controls below U+0020);
  non-ASCII is raw UTF-8, as `JSON.stringify` writes it;
- diffs (before/after) are **not** part of the hash.

Golden vector v1 (from #1333), count 3, irreversible true:

```
[{"group":"composition.krateo.io","irreversible":false,"name":"example-a","namespace":"example-ns","resource":"examplecatalogs","verb":"POST","version":"v1-2-0"},{"group":"","irreversible":true,"name":"example-b","namespace":"example-ns","resource":"configmaps","verb":"DELETE","version":"v1"},{"group":"apps","irreversible":false,"name":"","namespace":"example-ns","resource":"deployments","verb":"POST","version":"v1"}]
sha256 0a08bb91090b111f15a045b6061bccc4d0b8488d85f99ba57d8632e17061c277
```

Escaping vector: one op `{group:"", irreversible:true, name:"café- <&>\"\\\t\u0001",
namespace:"ns-é", resource:"configmaps", verb:"DELETE", version:"v1"}`,
`JSON.stringify` of the array, sha256
`c7d3aedeac8dd069c50a82a9be6540050c026b8b3230fe702e6a53ac97e6b377` (`<`, `&`, `>`
and U+2028 stay raw; tab is `\t`, U+0001 is `\u0001`).

Snowplow records the hash the header carries; it does not recompute it (it
never sees the set). `TestProvenance1333_G_GoldenVector` rebuilds both vectors'
bytes from their ops by these rules and checks both hashes, so the portal and
snowplow prove the same definition.

## 8. What the record holds

On the write's (or dry run's) record, beside what snowplow established itself
(`user.name`, `krateo.audit.actor_class`, `krateo.audit.verb`, the
`krateo.audit.object_ref.*` target, `k8s.namespace.name`,
`krateo.audit.outcome`, `http.response.status_code`, the event time):

| key | from |
|---|---|
| `krateo.audit.client.source` | `source` |
| `krateo.audit.client.agent_session_id` | `agentSessionId`, when sent |
| `krateo.audit.client.confirmation_id` | `confirmationId`, when sent |
| `krateo.audit.client.confirmed_set.count`, `.irreversible`, `.sha256`, `.op` | `confirmedSet` and `op`, all four or none |
| `krateo.audit.client.requested_at` | `requestedAt` in UTC with milliseconds, when sent; never the event time |
| `krateo.audit.client.rejected` | the reason, alone, when the header was rejected |

Every one is **asserted**: the client's word, kept apart from what snowplow
verified, and none is a policy key. The header value is never logged (not even
with the debug wire dump on), never put on a span, and never forwarded to the
apiserver; no identity is put on a span (`TestCall1333_S_*`). Keep the
`krateo.io/created-by` label on agent creates: it is on the object, the header
is on the event.
