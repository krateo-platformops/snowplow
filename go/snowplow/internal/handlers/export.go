package handlers

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jqutil"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
	"github.com/krateo-platformops/snowplow/internal/support/pdftable"
)

// exportParams are the /export-specific query parameters, stripped from
// the query before the request is re-dispatched through the /call lane.
var exportParams = []string{"format", "path", "fields", "filename", "title", "header"}

// Internal full-list pagination for an /export request that carries NO
// page/perPage of its own (the scheduled-export shape). A single bare
// /call re-dispatch would leave pagination to the target's own defaults —
// silently truncating the export to whatever default page size the
// RESTAction/widget applies. Instead the handler paginates-and-concats:
// it loops the /call lane at perPage=exportPageSize and concatenates the
// extracted rows until a short page signals exhaustion, bounded at
// exportMaxPages pages.
//
// DOCUMENTED CAP: exportPageSize*exportMaxPages rows (500*100 = 50000).
// An export that hits the bound is truncated and the response carries
// `X-Export-Truncated: true` so a consumer can tell a complete export
// from a capped one. Package vars (not consts) so tests can shrink them.
var (
	exportPageSize = 500
	exportMaxPages = 100
)

// exportTruncatedHeader flags an export that hit the exportMaxPages
// bound: the row set is a prefix of the full list, not the whole of it.
const exportTruncatedHeader = "X-Export-Truncated"

// exportBoundStatus answers a pdf export whose full list reaches the
// exportPageSize*exportMaxPages bound. csv and json send such a list
// truncated and flagged (exportTruncatedHeader); a pdf is a finished
// report with no header a reader could check, so it is never sent
// truncated: the request is refused with a JSON Status instead.
const exportBoundStatus = http.StatusUnprocessableEntity

// Export returns the GET /export handler: a GENERIC serializer layered on
// top of the /call resolve lane. It re-dispatches the request through the
// given /call handler chain in-process (same RBAC gate, same serve-time
// user-aware filtering, same cache), extracts the row set from the
// resolved envelope and streams it as a CSV or JSON attachment. Because
// it is content-agnostic, ANY list/table widget or RESTAction resolvable
// via /call is exportable — nothing per-widget, nothing per-service.
//
// Query parameters (in addition to the /call ones):
//
//   - format:   "csv" (default), "json" or "pdf"
//   - path:     optional jq expression selecting the rows within the
//     resolved envelope (e.g. ".status.items"); when omitted the
//     rows are auto-detected (top-level array, then .items, then
//     the first array under .status / .status.widgetData)
//   - fields:   optional comma-separated list of dot-paths selecting and
//     ordering the CSV/PDF columns (default: union of flattened keys)
//   - header:   optional column label, repeated once per field in fields
//     order (header=Name&header=Created%2C%20UTC); the CSV and PDF header
//     row, and the json columns[].label (see parseHeaderLabels, labeledJSON)
//   - title:    pdf only: the report title (default: the resource name);
//     ignored by csv and json
//   - filename: optional attachment filename (sanitized; extension added)
//
// Pagination semantics: a caller that supplies page/perPage exports
// exactly that /call window (single dispatch). A caller that omits BOTH
// gets the FULL list: the handler paginates the /call lane internally
// (perPage=exportPageSize) and concatenates the pages until exhaustion,
// bounded at exportMaxPages pages (exportPageSize*exportMaxPages rows);
// hitting the bound sets `X-Export-Truncated: true`. See collectAllPages.
// A pdf export never ships truncated: reaching the bound is an
// exportBoundStatus error (see servePDF).
//
// Every export emits an AuditEvent (action=export) so data egress is
// correlated end-to-end like any other action.
func Export(call http.Handler) http.Handler {
	return &exportHandler{call: call}
}

var _ http.Handler = (*exportHandler)(nil)

type exportHandler struct {
	// call is the composed GET /call handler chain (dispatcher included);
	// the export handler is a pure serializer on top of it.
	call http.Handler
}

// @Summary Export Endpoint
// @Description Export any /call-resolvable list as CSV, JSON or PDF
// @ID export
// @Param  apiVersion  query  string  true   "Resource API Group and Version"
// @Param  resource    query  string  true   "Resource Plural"
// @Param  name        query  string  true   "Resource name"
// @Param  namespace   query  string  true   "Resource namespace"
// @Param  format      query  string  false  "Export format: csv (default), json or pdf"
// @Param  path        query  string  false  "JQ expression selecting the rows in the resolved envelope"
// @Param  fields      query  string  false  "Comma separated list of column dot-paths"
// @Param  header      query  []string  false  "Column label, one per field in fields order (repeat the parameter); may repeat. The CSV and PDF header row; json becomes {columns:[{field,label}],rows:[{<field>:value}]}"  collectionFormat(multi)
// @Param  title       query  string  false  "PDF report title (default: the resource name); ignored by csv and json"
// @Param  filename    query  string  false  "Attachment file name"
// @Param  page        query  int     false  "Explicit /call page to export; omit (with perPage) to export the FULL list via internal pagination (capped, see X-Export-Truncated)"
// @Param  perPage     query  int     false  "Explicit /call page size; omit (with page) to export the FULL list via internal pagination"
// @Produce  text/csv
// @Produce  json
// @Produce  application/pdf
// @Success 200 {string} string
// @Failure 400 {object} response.Status
// @Failure 401 {object} response.Status
// @Failure 404 {object} response.Status
// @Failure 422 {object} response.Status "pdf only: the full list reaches the 50,000-row export bound"
// @Failure 500 {object} response.Status
// @Router /export [get]
func (r *exportHandler) ServeHTTP(wri http.ResponseWriter, req *http.Request) {
	log := xcontext.Logger(req.Context())

	format := strings.ToLower(req.URL.Query().Get("format"))
	if format == "" {
		format = "csv"
	}
	if format != "csv" && format != "json" && format != "pdf" {
		writeExportError(wri, http.StatusBadRequest,
			fmt.Errorf("unsupported export format %q (want csv, json or pdf)", format))
		return
	}

	rowsPath := req.URL.Query().Get("path")
	fields := parseFields(req.URL.Query().Get("fields"))
	labels, err := parseHeaderLabels(req.URL.Query()["header"], fields)
	if err != nil {
		writeExportError(wri, http.StatusBadRequest, err)
		return
	}

	// Pagination (review fix — silent truncation): an /export carrying an
	// explicit page/perPage exports exactly that /call window, unchanged.
	// An /export carrying NEITHER used to re-dispatch a single bare /call
	// and inherit the target's own default page size — silently truncating
	// the export. Now the bare shape paginates-and-concats the full list
	// (bounded; see collectAllPages).
	callerPaginated := req.URL.Query().Get("perPage") != "" ||
		req.URL.Query().Get("page") != ""

	var (
		rows      []any
		pages     int
		truncated bool
	)
	if callerPaginated {
		rec := r.dispatchCall(req, 0, 0)
		if rec.status != http.StatusOK {
			passThroughCallFailure(wri, rec)
			return
		}
		var code int
		var err error
		rows, code, err = extractFromEnvelope(req.Context(), rec.body.Bytes(), rowsPath)
		if err != nil {
			writeExportError(wri, code, err)
			return
		}
		pages = 1
	} else {
		var failed *exportRecorder
		var code int
		var err error
		rows, pages, truncated, failed, code, err = r.collectAllPages(req, rowsPath)
		if failed != nil {
			passThroughCallFailure(wri, failed)
			return
		}
		if err != nil {
			writeExportError(wri, code, err)
			return
		}
	}

	name := req.URL.Query().Get("name")
	filename := attachmentName(req.URL.Query().Get("filename"), name, format)
	auditExport := func(code int, err error) {
		outcome := "success"
		if err != nil {
			outcome = "failure"
		}
		audit.Emit(req.Context(), audit.Event{
			Action:    "export",
			Verb:      http.MethodGet,
			Resource:  req.URL.Query().Get("resource"),
			Name:      name,
			Namespace: req.URL.Query().Get("namespace"),
			Outcome:   outcome,
			Code:      code,
			Message: fmt.Sprintf("format=%s rows=%d pages=%d truncated=%t",
				format, len(rows), pages, truncated),
		})
	}

	if format == "pdf" {
		code, err := servePDF(wri, req, rows, fields, labels, truncated, filename)
		if err != nil {
			log.Error("unable to serve pdf export", slog.Int("code", code), slog.Any("err", err))
		}
		auditExport(code, err)
		return
	}

	wri.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	if truncated {
		wri.Header().Set(exportTruncatedHeader, "true")
	}

	switch format {
	case "json":
		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		enc := json.NewEncoder(wri)
		enc.SetIndent("", "  ")
		if labels != nil {
			err = enc.Encode(labeledBody(rows, fields, labels))
		} else {
			err = enc.Encode(rows)
		}
	default:
		wri.Header().Set("Content-Type", "text/csv; charset=utf-8")
		wri.WriteHeader(http.StatusOK)
		err = writeCSV(wri, rows, fields, labels)
	}
	if err != nil {
		log.Error("unable to serialize export", slog.Any("err", err))
	}
	auditExport(http.StatusOK, err)
}

// servePDF renders the rows as a PDF report and sends it. Nothing is
// written to wri until the whole document has been built: a list that
// reaches the bound, or a render failure, is answered with a JSON Status
// and its code, never with a 200 and a partial or broken file. It returns
// the status it answered with and, on failure, why.
//
// The report states the caller's username: the PDF body carries it on
// purpose (who generated this report). It is never logged, and no error
// returned here contains it.
func servePDF(wri http.ResponseWriter, req *http.Request, rows []any, fields, labels []string, truncated bool, filename string) (int, error) {
	if truncated {
		err := fmt.Errorf("the list reaches the export bound of %d rows and a pdf export is never truncated: "+
			"export it as csv or json (sent truncated and flagged %s), or a page at a time with page/perPage",
			exportPageSize*exportMaxPages, exportTruncatedHeader)
		writeExportError(wri, exportBoundStatus, err)
		return exportBoundStatus, err
	}
	user, err := xcontext.UserInfo(req.Context())
	if err != nil {
		err = fmt.Errorf("the request carries no caller identity: %w", err)
		writeExportError(wri, http.StatusInternalServerError, err)
		return http.StatusInternalServerError, err
	}

	cols, cells := cellTable(rows, fields)
	header := cols
	if labels != nil {
		header = labels
	}
	title := req.URL.Query().Get("title")
	if title == "" {
		title = req.URL.Query().Get("name")
	}
	now := time.Now().UTC()
	doc, err := pdftable.Render(pdftable.Document{
		Title:   title,
		Created: now,
		Meta: []string{
			"Generated: " + now.Format(time.RFC3339),
			"User: " + user.Username,
			fmt.Sprintf("Rows: %d", len(rows)),
		},
		Header: header,
		Rows:   cells,
	})
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, pdftable.ErrHeaderTooTall) {
			code = http.StatusBadRequest // the caller's labels or fields; shorter ones render
		}
		writeExportError(wri, code, err)
		return code, err
	}

	wri.Header().Set("Content-Type", "application/pdf")
	wri.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	wri.Header().Set("Content-Length", strconv.Itoa(len(doc)))
	wri.Header().Set("X-Content-Type-Options", "nosniff")
	wri.WriteHeader(http.StatusOK)
	if _, err := wri.Write(doc); err != nil {
		return http.StatusOK, fmt.Errorf("send pdf: %w", err)
	}
	return http.StatusOK, nil
}

// dispatchCall re-dispatches the request through the /call lane
// in-process: identical auth, RBAC and resolve semantics — export can
// never see more than the caller's own /call would return. The
// export-specific params are stripped; a page > 0 OVERRIDES the inner
// pagination with the given (page, perPage) window (the
// paginate-and-concat loop), otherwise the caller's own pagination
// params pass through untouched.
func (r *exportHandler) dispatchCall(req *http.Request, page, perPage int) *exportRecorder {
	inner := req.Clone(req.Context())
	q := inner.URL.Query()
	for _, p := range exportParams {
		q.Del(p)
	}
	if page > 0 {
		q.Set("page", strconv.Itoa(page))
		q.Set("perPage", strconv.Itoa(perPage))
	}
	inner.URL.RawQuery = q.Encode()

	rec := newExportRecorder()
	r.call.ServeHTTP(rec, inner)
	return rec
}

// collectAllPages is the paginate-and-concat lane for an unpaginated
// /export (the scheduled-export shape): it walks the /call lane page by
// page at perPage=exportPageSize and concatenates the extracted rows.
//
// Stop conditions:
//   - a page with a row count OTHER than exportPageSize — a short page
//     means the target sliced the window and this is the last page; an
//     overfull page means it ignored the slice and returned the full set
//     in one shot; either way the set is exhausted;
//   - a page identical to the previous one (fingerprint) — the target
//     ignores pagination but happens to return exactly exportPageSize
//     rows; the duplicate page is discarded, guarding against unbounded
//     duplication;
//   - the exportMaxPages bound (documented cap of
//     exportPageSize*exportMaxPages rows) — the export is truncated and
//     flagged via exportTruncatedHeader.
//
// A non-200 inner page aborts the export; the /call failure envelope is
// returned via failed for pass-through. On extraction errors code is the
// HTTP status to respond with.
func (r *exportHandler) collectAllPages(req *http.Request, rowsPath string) (rows []any, pages int, truncated bool, failed *exportRecorder, code int, err error) {
	prevFP := ""
	for page := 1; page <= exportMaxPages; page++ {
		rec := r.dispatchCall(req, page, exportPageSize)
		if rec.status != http.StatusOK {
			return nil, 0, false, rec, 0, nil
		}
		pageRows, pcode, perr := extractFromEnvelope(req.Context(), rec.body.Bytes(), rowsPath)
		if perr != nil {
			return nil, 0, false, nil, pcode, perr
		}
		fp := rowsFingerprint(pageRows)
		if page > 1 && fp == prevFP {
			// The target ignores the injected pagination: every page is
			// the same set. Keep the single copy already accumulated.
			break
		}
		prevFP = fp
		rows = append(rows, pageRows...)
		pages = page
		if len(pageRows) != exportPageSize {
			break // last (short) page, or a slice-ignoring full set
		}
		if page == exportMaxPages {
			truncated = true // full last-allowed page: more may exist
		}
	}
	return rows, pages, truncated, nil, 0, nil
}

// extractFromEnvelope decodes a /call response body and extracts the row
// set (explicit jq path or auto-detect). The int return is the HTTP
// status to respond with when err != nil.
func extractFromEnvelope(ctx context.Context, body []byte, rowsPath string) ([]any, int, error) {
	var doc any
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, http.StatusInternalServerError,
			fmt.Errorf("resolved envelope is not JSON: %w", err)
	}
	rows, err := extractRows(ctx, doc, rowsPath)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	return rows, 0, nil
}

// rowsFingerprint is the page-identity fingerprint the
// paginate-and-concat loop uses to detect a pagination-ignoring target.
// rows always round-trip json.Unmarshal → json.Marshal, so the error is
// structurally impossible here.
func rowsFingerprint(rows []any) string {
	dat, _ := json.Marshal(rows)
	return string(dat)
}

// passThroughCallFailure relays a non-200 /call envelope. The /call lane
// always emits a JSON Status object (see call.go), so the relayed body is
// pinned to application/json and marked nosniff: it can never be
// interpreted as HTML by a browser, closing the reflected-XSS sink a taint
// analyzer sees on the body write.
func passThroughCallFailure(wri http.ResponseWriter, rec *exportRecorder) {
	for k, vv := range rec.header {
		if strings.EqualFold(k, "Content-Type") {
			continue // pinned below; never relay a text/html content-type
		}
		for _, v := range vv {
			wri.Header().Add(k, v)
		}
	}
	wri.Header().Set("Content-Type", "application/json")
	wri.Header().Set("X-Content-Type-Options", "nosniff")
	wri.WriteHeader(rec.status)
	// FALSE POSITIVE (reviewed, PR #116). gosec's taint analysis flags this body
	// write as reflected XSS, but the sink is closed: the /call lane always emits
	// a JSON Status object (call.go) and the response is pinned to Content-Type:
	// application/json + X-Content-Type-Options: nosniff two lines above, so a
	// browser can never interpret the body as HTML. The analyzer does not model
	// the content-type/nosniff mitigation. The genuine egress-injection risk (CSV
	// formula injection on the SUCCESS path) is fixed separately in
	// neutralizeCSVCell.
	_, _ = wri.Write(rec.body.Bytes()) // #nosec
}

// extractRows locates the row set inside the resolved envelope: an
// explicit jq path wins; otherwise common list shapes are auto-detected.
func extractRows(ctx context.Context, doc any, rowsPath string) ([]any, error) {
	if rowsPath != "" {
		s, err := jqutil.Eval(ctx, jqutil.EvalOptions{
			Query:        rowsPath,
			Data:         doc,
			ModuleLoader: jqsupport.ModuleLoader(),
		})
		if err != nil {
			return nil, fmt.Errorf("unable to evaluate rows path %q: %w", rowsPath, err)
		}
		var v any
		if err := json.Unmarshal([]byte(s), &v); err != nil {
			v = s // bare string result
		}
		return normalizeRows(v), nil
	}

	return autoDetectRows(doc), nil
}

// autoDetectRows walks the conventional resolved shapes looking for the
// row array: top-level array → .items → .status (array, .items, or the
// first array found under .status / .status.widgetData in sorted key
// order). Falls back to a single-row export of the whole document.
func autoDetectRows(doc any) []any {
	if rows, ok := doc.([]any); ok {
		return rows
	}

	m, ok := doc.(map[string]any)
	if !ok {
		return []any{doc}
	}

	if rows, ok := m["items"].([]any); ok {
		return rows
	}

	if status, ok := m["status"]; ok {
		if rows, ok := status.([]any); ok {
			return rows
		}
		if sm, ok := status.(map[string]any); ok {
			if rows, ok := sm["items"].([]any); ok {
				return rows
			}
			if wd, ok := sm["widgetData"].(map[string]any); ok {
				if rows, ok := firstArray(wd); ok {
					return rows
				}
			}
			if rows, ok := firstArray(sm); ok {
				return rows
			}
		}
	}

	return []any{doc}
}

// firstArray returns the value of the first (sorted-key order,
// deterministic) top-level key holding a non-empty array.
func firstArray(m map[string]any) ([]any, bool) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if rows, ok := m[k].([]any); ok && len(rows) > 0 {
			return rows, true
		}
	}
	return nil, false
}

func normalizeRows(v any) []any {
	switch t := v.(type) {
	case nil:
		return []any{}
	case []any:
		return t
	default:
		return []any{v}
	}
}

// flattenRows flattens every row (flattenLeaves) and resolves the column
// set/order: the explicit fields list when given, else the sorted union of
// the flattened keys across all rows. csv, the labelled json and pdf all
// select their columns through it.
func flattenRows(rows []any, fields []string) (cols []string, flat []map[string]any) {
	flat = make([]map[string]any, 0, len(rows))
	colSet := map[string]struct{}{}
	for _, row := range rows {
		fr := map[string]any{}
		flattenLeaves(fr, "", row)
		for k := range fr {
			colSet[k] = struct{}{}
		}
		flat = append(flat, fr)
	}

	cols = fields
	if len(cols) == 0 {
		cols = make([]string, 0, len(colSet))
		for k := range colSet {
			cols = append(cols, k)
		}
		sort.Strings(cols)
	}
	return cols, flat
}

// writeCSV flattens each row (nested maps become dot-path columns,
// arrays are JSON-encoded) and writes an RFC 4180 CSV. Column set/order:
// see flattenRows. The header row is labels when given, else the column
// dot-paths.
func writeCSV(wri http.ResponseWriter, rows []any, fields, labels []string) error {
	cols, flat := flattenRows(rows, fields)
	header := cols
	if labels != nil {
		header = labels
	}

	w := csv.NewWriter(wri)
	if err := w.Write(header); err != nil {
		return err
	}
	rec := make([]string, len(cols))
	for _, fr := range flat {
		for i, c := range cols {
			rec[i] = csvCell(fr[c])
		}
		if err := w.Write(rec); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

// csvCell is a leaf's CSV text: cellText, plus CSV formula-injection
// neutralization (OWASP) on strings — a cell whose FIRST char is a formula
// trigger is prefixed with a single quote so a spreadsheet renders it as
// literal text, not an executable formula. The string case is the
// user-controlled-text vector (resource names/labels/status text flow
// here); numeric/bool cells are snowplow-produced and not attacker-shaped.
// CSV-only: JSON export is unaffected (its json.Encoder escapes), and a
// PDF cell is drawn text, never evaluated. PR #116 arch blocker.
func csvCell(leaf any) string {
	if s, ok := leaf.(string); ok {
		return neutralizeCSVCell(s)
	}
	return cellText(leaf)
}

// cellTable is the rows' cell text in column order (see flattenRows), for
// the pdf table.
func cellTable(rows []any, fields []string) (cols []string, cells [][]string) {
	cols, flat := flattenRows(rows, fields)
	cells = make([][]string, len(flat))
	for r, fr := range flat {
		cells[r] = make([]string, len(cols))
		for i, c := range cols {
			cells[r][i] = cellText(fr[c])
		}
	}
	return cols, cells
}

// flattenLeaves flattens nested maps to dot-path keys holding the leaves
// as decoded: scalars, null, arrays and empty maps.
func flattenLeaves(out map[string]any, prefix string, v any) {
	if t, ok := v.(map[string]any); ok && len(t) > 0 {
		for k, cv := range t {
			p := k
			if prefix != "" {
				p = prefix + "." + k
			}
			flattenLeaves(out, p, cv)
		}
		return
	}
	out[keyOrRoot(prefix)] = v
}

// cellText stringifies a leaf: strings as they are, bools and numbers in
// their JSON spelling, null (and a missing column) as empty, composite
// leaves (arrays, empty maps) JSON-encoded.
func cellText(leaf any) string {
	switch t := leaf.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		return fmt.Sprintf("%t", t)
	case float64:
		return strconv64(t)
	default:
		dat, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprintf("%v", t)
		}
		return string(dat)
	}
}

// labeledJSON is the json body of an export that carries header labels.
// Labels are display text and may repeat (two "Name" columns told apart by
// position); fields are the identifiers. So the labels never become keys:
// columns lists every selected column in fields order as {field, label} —
// a field selected twice (two views of one value) is listed twice — and
// each row is an object keyed by field, every field once, its keys in
// order of first selection, holding the selected (flattened) leaves as
// decoded (a missing column is null). A consumer pairs columns with a
// row's values by field. Without labels the json body is the extracted
// rows, unchanged.
type labeledJSON struct {
	Columns []labeledColumn `json:"columns"`
	Rows    []fieldRow      `json:"rows"`
}

type labeledColumn struct {
	Field string `json:"field"`
	Label string `json:"label"`
}

// fieldRow is one row keyed by field, written in fields order (a Go map
// would sort the keys).
type fieldRow struct {
	fields []string
	values []any
}

func (r fieldRow) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, f := range r.fields {
		if i > 0 {
			b.WriteByte(',')
		}
		k, err := json.Marshal(f)
		if err != nil {
			return nil, err
		}
		v, err := json.Marshal(r.values[i])
		if err != nil {
			return nil, err
		}
		b.Write(k)
		b.WriteByte(':')
		b.Write(v)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// labeledBody builds the json body of a labelled export (labeledJSON).
func labeledBody(rows []any, fields, labels []string) labeledJSON {
	out := labeledJSON{Columns: make([]labeledColumn, len(fields))}
	for i, f := range fields {
		out.Columns[i] = labeledColumn{Field: f, Label: labels[i]}
	}
	// Every field once, in order of first selection: two columns of one
	// field read the same cell, so the row carries the value once.
	keys := make([]string, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		if _, dup := seen[f]; !dup {
			seen[f] = struct{}{}
			keys = append(keys, f)
		}
	}
	_, flat := flattenRows(rows, fields)
	out.Rows = make([]fieldRow, len(flat))
	for r, fr := range flat {
		values := make([]any, len(keys))
		for i, f := range keys {
			values[i] = fr[f]
		}
		out.Rows[r] = fieldRow{fields: keys, values: values}
	}
	return out
}

func keyOrRoot(prefix string) string {
	if prefix == "" {
		return "value"
	}
	return prefix
}

// csvFormulaTriggers is the OWASP-standard set of leading characters a
// spreadsheet may interpret as the start of a formula when opening a CSV.
// A cell beginning with any of these is neutralized (prefixed with a single
// quote) so it renders as literal text. \t and \r are included because some
// spreadsheet importers strip leading whitespace and then re-trigger on the
// following formula char.
const csvFormulaTriggers = "=+-@\t\r"

// neutralizeCSVCell defends against CSV formula injection: if s begins with a
// formula-trigger char, it is prefixed with a single quote (the spreadsheet
// then shows the literal value, e.g. `=1+1` renders as text, not the computed
// 2). Empty strings and strings that begin with any other char are returned
// unchanged. This is the CSV success-path guard the export lacked (PR #116).
func neutralizeCSVCell(s string) string {
	if s == "" {
		return s
	}
	if strings.IndexByte(csvFormulaTriggers, s[0]) >= 0 {
		return "'" + s
	}
	return s
}

// strconv64 renders a JSON number without a spurious trailing ".000000".
func strconv64(f float64) string {
	if f == float64(int64(f)) {
		return fmt.Sprintf("%d", int64(f))
	}
	return fmt.Sprintf("%g", f)
}

// parseHeaderLabels validates the optional column labels: one `header`
// query parameter per selected field, in fields order. The encoding is a
// repeated parameter, not a comma list, so a label may hold any character,
// commas included (header=Created%2C%20UTC). A label is display text, used
// verbatim, and may repeat; the fields are the column identifiers, and a
// field may be selected twice (two views of one value). Labels pair with
// fields by position, so they need an explicit fields list of the same
// length. No header parameter returns nil: the columns are headed by their
// dot-paths, as before.
func parseHeaderLabels(raw, fields []string) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(fields) == 0 {
		return nil, errors.New("header labels need an explicit fields list: one header parameter per field, in the same order")
	}
	if len(raw) != len(fields) {
		return nil, fmt.Errorf("got %d header labels for %d fields: give one header parameter per field, in the same order", len(raw), len(fields))
	}
	return raw, nil
}

func parseFields(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

var unsafeFilenameChars = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func attachmentName(requested, resource, format string) string {
	base := unsafeFilenameChars.ReplaceAllString(requested, "-")
	base = strings.Trim(base, "-.")
	if base == "" {
		base = unsafeFilenameChars.ReplaceAllString(resource, "-")
		base = strings.Trim(base, "-.")
		if base == "" {
			base = "export"
		}
		base = fmt.Sprintf("%s-%s", base, time.Now().UTC().Format("20060102-150405"))
	}
	if !strings.HasSuffix(base, "."+format) {
		base += "." + format
	}
	return base
}

func writeExportError(wri http.ResponseWriter, code int, err error) {
	wri.Header().Set("Content-Type", "application/json")
	wri.WriteHeader(code)
	_ = json.NewEncoder(wri).Encode(map[string]any{
		"kind":    "Status",
		"status":  "Failure",
		"code":    code,
		"message": err.Error(),
	})
}

// exportRecorder is a minimal in-process http.ResponseWriter capturing the
// /call lane response for post-serialization.
type exportRecorder struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func newExportRecorder() *exportRecorder {
	return &exportRecorder{header: http.Header{}, status: http.StatusOK}
}

func (r *exportRecorder) Header() http.Header { return r.header }

func (r *exportRecorder) WriteHeader(code int) { r.status = code }

func (r *exportRecorder) Write(p []byte) (int, error) { return r.body.Write(p) }
