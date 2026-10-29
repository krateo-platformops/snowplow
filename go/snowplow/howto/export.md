---
type: Usage
title: Generic export (GET /export)
description: Export any /call-resolvable list (RESTAction or Widget) as a CSV, JSON or PDF attachment — parameters, column header labels, row auto-detection, full-list pagination and the truncation cap, CSV shaping, the PDF report, and the audit trail.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [portal, export, csv, pdf, audit]
timestamp: 2026-10-29T00:00:00Z
---

# Generic export (`GET /export`)

`/export` turns **any `/call`-resolvable list** — a `RESTAction` or a
list/table `Widget` — into a downloadable **CSV**, **JSON** or **PDF** attachment.
It is a pure serializer layered on the `/call` resolve lane
(`internal/handlers/export.go`): the request is re-dispatched **in-process**
through the same dispatcher chain the `GET /call` route uses, so
authentication, the RBAC gate and the serve-time user-aware filtering are
identical. An export can never contain more than the caller's own `/call`
would return.

Every export also emits an [`AuditEvent`](./audit-correlation.md)
(a trace-correlated OTLP LogRecord, `krateo.action=export`) carrying the
request session id (the W3C **`baggage`** member `session.id`), so data egress
is auditable end-to-end. (There is no bespoke `X-Krateo-Correlation-Id`
header — correlation rides W3C baggage.)

## Request

```
GET /export?apiVersion=<gv>&resource=<plural>&name=<name>&namespace=<ns>[&format=...][&path=...][&fields=...][&header=...]...[&title=...][&filename=...][&page=...&perPage=...]
```

The `apiVersion` / `resource` / `name` / `namespace` parameters are the
standard `/call` ones. Authentication is the same JWT used for `/call`.
The export parameters below are stripped before the request is
re-dispatched through `/call`.

| Parameter  | Default | Description |
|------------|---------|-------------|
| `format`   | `csv`   | `csv`, `json` or `pdf`. |
| `path`     | auto    | Optional **jq expression** selecting the row array inside the resolved envelope, e.g. `.status.services`. |
| `fields`   | all     | Comma-separated list of column **dot-paths** selecting and ordering the CSV and PDF columns, e.g. `name,health,usage.cpu`. |
| `header`   | dot-paths | Optional column **label**, one `header` parameter per field, repeated in `fields` order (below). |
| `title`    | resource name | `pdf` only: the report title. |
| `filename` | derived | Attachment file name (sanitized; extension appended). |
| `page` / `perPage` | full list | Export exactly that `/call` window. When **both** are omitted, the handler exports the **full list** (below). |

### Column header labels

`header` gives the selected columns human-readable labels. It is a
**repeated parameter, one per field, in the same order as `fields`** — not a
comma-separated list — so a label may contain any character, commas
included (URL-encode it):

```
fields=metadata.name,metadata.creationTimestamp&header=Name&header=Created%2C%20UTC
```

The labels are used verbatim as the CSV header row, the JSON keys and the
PDF header row. They require an explicit `fields` list of the same length
and must be unique (they become JSON keys); otherwise the request is a
`400` and nothing is dispatched. With labels, the JSON export is one object
per row whose keys are the labels, in `fields` order, and whose values are
the selected (flattened) values with their JSON types; a missing value is
`null`. Without `header`, csv and json are exactly as before: the CSV header
row is the dot-paths, and the JSON export is the extracted rows as resolved.

Labels are generic: snowplow has no widget-kind logic. To export a `Table`
widget's typed cells, select and shape them with your own `path` jq, then
name the columns with `fields` and `header`.

### Full-list pagination and the truncation cap

An `/export` that carries no `page`/`perPage` of its own (the scheduled-export
shape) does **not** inherit the target's default page size. The handler
paginates the `/call` lane internally (`perPage=500`) and concatenates the
extracted rows page by page until a short page signals exhaustion, bounded at
100 pages — a documented cap of **50 000 rows** (`collectAllPages`,
`export.go`). An export that hits the bound is truncated and the response
carries **`X-Export-Truncated: true`**, so a consumer can tell a complete
export from a capped one. A target that ignores the injected pagination is
detected by page fingerprinting and exported once, never duplicated.

**A PDF is never truncated.** A `format=pdf` full-list export that reaches
the bound is refused with **`422`** and a JSON `Status` body, and no PDF is
sent: a report has no header a reader could check for truncation. Export
such a list as csv or json, or a page at a time with `page`/`perPage`.

Because every internal page carries an explicit `page` **and** `perPage`, an
unpaginated export of a widget whose parent declares a smaller slice (for
example `perPage: 50` in its `resourcesRefs`) still exports the full list:
the explicit values override the declared slice.

### Row auto-detection

When `path` is omitted the rows are located by convention, in order
(`autoDetectRows`):

1. the resolved envelope itself, when it is an array;
2. `.items`;
3. `.status` when it is an array, then `.status.items`;
4. the first (sorted-key order) non-empty array under
   `.status.widgetData`, then under `.status`;
5. otherwise the whole envelope is exported as a single row.

### CSV shaping

Nested objects are flattened to dot-path columns (`usage.cpu`); arrays
and empty objects are JSON-encoded in place; the header row is the
sorted union of all flattened keys (or the explicit `fields` order).
String cells beginning with a spreadsheet formula trigger (`=`, `+`, `-`,
`@`, tab, CR) are prefixed with a single quote so a spreadsheet renders
them as literal text, not an executable formula (`neutralizeCSVCell` —
OWASP CSV-injection guard; the JSON format is unaffected).

### PDF report

`format=pdf` renders the same rows — the same RBAC-filtered `/call`
re-dispatch, the same `path`/auto-detection and the same `fields`
flattening, so a PDF never shows more than the caller's `/call` — as a
table report (`internal/support/pdftable`):

- the first page states the title (`title`, default the resource name),
  the generation time in UTC, the caller's username and the row count;
- the selected columns, in order, with the header row repeated at the top
  of every page, and a page number on every page;
- landscape A4; long cells wrap inside their column (at spaces, at line
  breaks, or between characters for a word wider than the column) and are
  never clipped; a row taller than a page continues on the next one;
  columns that do not all fit across the page at a minimum width are
  printed as bands of adjacent columns, each with all the rows;
- the Go fonts (regular and bold, BSD-3-Clause) embedded, so text in
  Latin, Greek and Cyrillic scripts renders and extracts as Unicode; a
  character the font has no glyph for (for example CJK) is drawn as a
  placeholder box and extracts as U+FFFD;
- no links, annotations, actions, scripts, forms or external references.

The document is built completely before any byte is sent: a failure is an
error status with a JSON `Status` body (for example `400` for column labels
too long to repeat on every page), never a `200` with a broken file. The
response is `application/pdf` with
`Content-Disposition: attachment; filename="<sanitized>.pdf"`.

The username is in the PDF on purpose (who generated the report); it is
never written to snowplow's logs. Building a 50,000-row × 10-column PDF
peaks at about 165 MiB of heap, against about 128 MiB for the same export
as CSV (`BenchmarkExport970_50Kx10_*`, `export_pdf_bench_test.go`).

## Examples

```sh
# CSV of a RESTAction list, columns picked and ordered explicitly
curl -H "Authorization: Bearer $JWT" \
  "$SNOWPLOW/export?apiVersion=templates.krateo.io/v1&resource=restactions&name=service-list&namespace=demo&fields=name,health,usage.pct"

# JSON of a table widget's rows
curl -H "Authorization: Bearer $JWT" \
  "$SNOWPLOW/export?apiVersion=widgets.templates.krateo.io/v1beta1&resource=tables&name=my-table&namespace=demo&format=json"

# jq-selected rows out of a custom envelope
curl -H "Authorization: Bearer $JWT" \
  "$SNOWPLOW/export?apiVersion=templates.krateo.io/v1&resource=restactions&name=usage&namespace=demo&path=.status.records&format=json"

# PDF report with a title and labelled columns (a label may contain commas)
curl -fsS -H "Authorization: Bearer $JWT" -o services.pdf \
  "$SNOWPLOW/export?apiVersion=templates.krateo.io/v1&resource=restactions&name=service-list&namespace=demo&format=pdf&title=Services%20by%20health&fields=name,health,usage.pct&header=Service&header=Health&header=Usage%2C%20%25"
```

Each export's `AuditEvent` carries the real outcome and status code and a
message of the form `format=<csv|json|pdf> rows=<n> pages=<n>
truncated=<bool>`; a refused or failed pdf export is recorded with
`outcome=failure` and its error code.

## Scheduled / recurring exports

A scheduled export is deliberately **just a CronJob calling `/export`**
— no extra controller, no export-specific state. The CronJob runs with
its own (least-privilege) credentials, so the recurring export is
RBAC-scoped exactly like an interactive one, and each run emits its own
`AuditEvent`. Where the file goes (S3-compatible object storage, a PVC,
an email gateway, ...) is deployment configuration, not snowplow's
concern.

See [`manifests/scheduled-export.cronjob.example.yaml`](../manifests/scheduled-export.cronjob.example.yaml)
for a complete, generic example that uploads the CSV to any
S3-compatible bucket.
