//go:build js && wasm

package fakeapiserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/kubernetes/scheme"
	kjson "sigs.k8s.io/json"
)

var listOptionsKind = schema.GroupKind{Group: "meta.k8s.io", Kind: "ListOptions"}

func fieldErrors(path, typ, msg string) field.ErrorList {
	p := field.NewPath(path)
	if typ == "Required" {
		return field.ErrorList{field.Required(p, msg)}
	}
	return field.ErrorList{field.Forbidden(p, msg)}
}

const maxBody = 3 << 20 // the apiserver's default request body limit (3 MiB)

// normalize is the stored form of a JSON value: decoded with json.Number, so numbers keep their
// literal and a no-op write compares equal to what is stored.
func normalize(v any) (map[string]any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return decodeJSONObject(b)
}

func decodeJSONObject(b []byte) (map[string]any, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var m map[string]any
	if err := d.Decode(&m); err != nil {
		return nil, err
	}
	if d.More() {
		return nil, errors.New("trailing data after the object")
	}
	if m == nil {
		return nil, errors.New("not an object")
	}
	return m, nil
}

func readBody(r *http.Request) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}
	if len(b) > maxBody {
		return nil, apierrors.NewRequestEntityTooLargeError("limit is 3145728")
	}
	return b, nil
}

func mediaType(r *http.Request) string {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return ""
	}
	return mt
}

// typed reports whether client-go's scheme registers the resource's kind. Such a kind is decoded
// through it and stored in its typed form's JSON (quantities canonical, as the apiserver stores
// them); any other kind (a custom resource, or one the apiserver serves that the scheme lacks:
// CustomResourceDefinitions, APIServices) is JSON, as unstructured.
func (s *Server) typed(c *reqCtx) bool {
	return !s.custom[c.gr] && scheme.Scheme.Recognizes(schema.GroupVersionKind{Group: c.group, Version: c.version, Kind: c.api.Kind})
}

// decodeObject reads a create or update body, with the strict serializer under fieldValidation
// Warn or Strict (create.go:117-140): its strict decoding errors are returned for strictOutcome,
// with the object. A protobuf body of a kind the scheme does not register is 415.
func (s *Server) decodeObject(c *reqCtx, body []byte, fv string) (map[string]any, []error, error) {
	mt := mediaType(c.r)
	if mt == "" {
		mt = "application/json"
	}
	switch mt {
	case "application/json", "application/yaml", "application/vnd.kubernetes.protobuf":
	default:
		return nil, nil, unsupportedMediaType(s.bodyTypes(c))
	}
	builtin := s.typed(c)
	if mt == "application/vnd.kubernetes.protobuf" && !builtin {
		return nil, nil, unsupportedMediaType(s.bodyTypes(c))
	}
	if builtin {
		obj, strict, err := decodeBuiltin(mt, body, fv)
		if err != nil {
			if mt == "application/vnd.kubernetes.protobuf" {
				return nil, nil, unsupportedMediaType(s.bodyTypes(c))
			}
			return nil, nil, apierrors.NewBadRequest(fmt.Sprintf("the object provided is unrecognized: %v", err))
		}
		return obj, strict, nil
	}
	if mt == "application/yaml" {
		return nil, nil, s.unimplemented("a YAML body for a kind client-go's scheme does not register")
	}
	obj, err := decodeJSONObject(body)
	if err != nil {
		return nil, nil, apierrors.NewBadRequest(fmt.Sprintf("the object provided is unrecognized: %v", err))
	}
	var strict []error
	if strictDecoding(fv) {
		// The strict JSON serializer on an unstructured object reports duplicate fields; unknown
		// ones are the schema's to find (coerceCustomResource).
		strict, _ = kjson.UnmarshalStrict(body, &map[string]any{}, kjson.DisallowDuplicateFields)
	}
	return obj, strict, nil
}

// decodeBuiltin decodes a kind client-go's scheme registers with the codec factory's serializer for
// the media type, its strict serializer under Warn or Strict (protobuf has none), and returns the
// typed form as JSON with the strict decoding errors.
func decodeBuiltin(mt string, body []byte, fv string) (map[string]any, []error, error) {
	info, ok := runtime.SerializerInfoForMediaType(scheme.Codecs.SupportedMediaTypes(), mt)
	if !ok {
		return nil, nil, fmt.Errorf("no serializer for %s", mt)
	}
	dec := info.Serializer
	if strictDecoding(fv) && info.StrictSerializer != nil {
		dec = info.StrictSerializer
	}
	obj, gvk, err := dec.Decode(body, nil, nil)
	var strict []error
	if err != nil {
		se, isStrict := runtime.AsStrictDecodingError(err)
		if !isStrict || obj == nil {
			return nil, nil, err
		}
		strict = se.Errors()
	}
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, nil, err
	}
	u["apiVersion"], u["kind"] = gvk.GroupVersion().String(), gvk.Kind
	n, err := normalize(u)
	return n, strict, err
}

// decodeCustom runs a custom resource's decoding (coerceCustomResource) and answers fieldValidation
// for the write's strict errors (prior ones first: the body's or the patch's own).
func (s *Server) decodeCustom(c *reqCtx, obj map[string]any, fv string, patched []byte, prior []error) (map[string]any, error) {
	out, strict, err := s.coerceCustomResource(c, obj, fv)
	if err != nil {
		if c.verb == "patch" {
			return nil, patchDecodeError(patched, err)
		}
		return nil, decodeError(c, obj, err)
	}
	if err := strictOutcome(c, fv, obj, patched, append(prior, strict...)); err != nil {
		return nil, err
	}
	return out, nil
}

func meta(obj map[string]any) map[string]any {
	md, _ := obj["metadata"].(map[string]any)
	if md == nil {
		md = map[string]any{}
		obj["metadata"] = md
	}
	return md
}

func (s *Server) hasStatus(c *reqCtx) bool {
	_, ok := s.gvs[c.gv][c.resource+"/status"]
	return ok
}

func (s *Server) gvr(c *reqCtx) schema.GroupResource {
	return schema.GroupResource{Group: c.group, Resource: c.resource}
}

func (s *Server) ensureNamespace(c *reqCtx) error {
	if c.ns == "" {
		return nil
	}
	if s.bucket("/namespaces")["/"+c.ns] == nil {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, c.ns)
	}
	return nil
}

// commit stores obj at the next resourceVersion and emits the event. Holds s.mu.
//
// A dry run (dryRun=All) has run the whole write path to here and commits nothing: no RV bump, no
// event, no stored change (k8s.io/apiserver v0.36.5 pkg/registry/generic/registry/dryrun.go: a
// dry-run create answers without a resourceVersion, an update keeps the stored one).
func (s *Server) commit(c *reqCtx, typ string, obj, prev map[string]any) map[string]any {
	if c.dryRun {
		switch typ {
		case "ADDED":
			delete(meta(obj), "resourceVersion")
		case "MODIFIED":
			meta(obj)["resourceVersion"] = meta(prev)["resourceVersion"]
		}
		return obj
	}
	s.rv++
	meta(obj)["resourceVersion"] = strconv.FormatUint(s.rv, 10)
	key := c.ns + "/" + fmt.Sprint(meta(obj)["name"])
	if typ == "DELETED" {
		delete(s.bucket(c.gr), key)
	} else {
		s.bucket(c.gr)[key] = obj
	}
	if c.group == "rbac.authorization.k8s.io" {
		s.reloadRBAC()
	}
	if c.gr == "apiextensions.k8s.io/customresourcedefinitions" && typ != "DELETED" {
		if gr, info, err := loadCRD(obj); err == nil {
			s.crds[gr], s.custom[gr] = info, true
		}
	}
	s.emit(c.gr, typ, obj, prev)
	return obj
}

func (s *Server) reloadRBAC() {
	var objs []map[string]any
	for gr, b := range s.store {
		if strings.HasPrefix(gr, "rbac.authorization.k8s.io/") {
			for _, o := range b {
				objs = append(objs, o)
			}
		}
	}
	if a, err := newAuthorizer(objs); err == nil {
		s.authz = a
	}
}

// restrict keeps what a write may not change, as the apiserver's strategies do: server-set metadata
// always; with a status subresource, the main resource keeps the stored status, and /status keeps
// everything but status.
func (s *Server) restrict(c *reqCtx, prev, next map[string]any) map[string]any {
	if s.hasStatus(c) {
		if c.subresource == "status" {
			out := deepCopy(prev).(map[string]any)
			if st, ok := next["status"]; ok {
				out["status"] = st
			} else {
				delete(out, "status")
			}
			return out
		}
		if st, ok := prev["status"]; ok {
			next["status"] = deepCopy(st)
		} else {
			delete(next, "status")
		}
	}
	pm, nm := meta(prev), meta(next)
	for _, k := range []string{"uid", "creationTimestamp", "resourceVersion", "generation", "deletionTimestamp", "deletionGracePeriodSeconds"} {
		if v, ok := pm[k]; ok {
			nm[k] = v
		} else {
			delete(nm, k)
		}
	}
	return next
}

// generationKinds are the built-in kinds whose strategy sets metadata.generation (k8s.io/kubernetes
// v1.36.2 pkg/registry/<group>/<kind>/strategy.go, PrepareForCreate: Generation = 1). Custom
// resources always have it (apiextensions-apiserver pkg/registry/customresource/strategy.go).
// Any other kind has no generation.
var generationKinds = map[string]bool{
	"admissionregistration.k8s.io/mutatingadmissionpolicies": true, "admissionregistration.k8s.io/mutatingadmissionpolicybindings": true,
	"admissionregistration.k8s.io/mutatingwebhookconfigurations": true, "admissionregistration.k8s.io/validatingadmissionpolicies": true,
	"admissionregistration.k8s.io/validatingadmissionpolicybindings": true, "admissionregistration.k8s.io/validatingwebhookconfigurations": true,
	"apps/daemonsets": true, "apps/deployments": true, "apps/replicasets": true, "apps/statefulsets": true,
	"batch/cronjobs": true, "batch/jobs": true, "/pods": true, "/podtemplates": true, "/replicationcontrollers": true,
	"discovery.k8s.io/endpointslices": true, "flowcontrol.apiserver.k8s.io/flowschemas": true,
	"flowcontrol.apiserver.k8s.io/prioritylevelconfigurations": true, "networking.k8s.io/ingresses": true,
	"networking.k8s.io/ingressclasses": true, "networking.k8s.io/networkpolicies": true, "policy/poddisruptionbudgets": true,
	"resource.k8s.io/deviceclasses": true, "resource.k8s.io/devicetaintrules": true, "resource.k8s.io/resourceslices": true,
	"scheduling.k8s.io/priorityclasses": true,
}

func (s *Server) hasGeneration(c *reqCtx) bool { return s.custom[c.gr] || generationKinds[c.gr] }

// bumpGeneration: for a kind that has a generation, metadata.generation increments when anything
// but metadata (and, with a status subresource, status) changes.
func (s *Server) bumpGeneration(c *reqCtx, prev, next map[string]any) {
	if !s.hasGeneration(c) {
		return
	}
	strip := func(o map[string]any) map[string]any {
		x := deepCopy(o).(map[string]any)
		delete(x, "metadata")
		if s.hasStatus(c) {
			delete(x, "status")
		}
		return x
	}
	if !reflect.DeepEqual(strip(prev), strip(next)) {
		g, _ := strconv.ParseInt(fmt.Sprint(meta(prev)["generation"]), 10, 64)
		if g < 1 {
			g = 1
		}
		meta(next)["generation"] = json.Number(strconv.FormatInt(g+1, 10))
	}
}

// write stores a changed object; a no-op stores nothing, emits nothing and keeps the RV. Holds s.mu.
func (s *Server) write(c *reqCtx, prev, next map[string]any) (map[string]any, error) {
	next, err := normalize(next)
	if err != nil {
		return nil, apierrors.NewInternalError(err)
	}
	if reflect.DeepEqual(prev, next) {
		return prev, nil
	}
	if c.subresource != "status" {
		s.bumpGeneration(c, prev, next)
	}
	// k8s.io/apiserver v0.36.5 pkg/registry/generic/registry/store.go ShouldDeleteDuringUpdate
	// (:568-580): an object marked for deletion whose last finalizer is removed is deleted.
	if _, deleting := meta(next)["deletionTimestamp"]; deleting {
		if fin, _ := meta(next)["finalizers"].([]any); len(fin) == 0 {
			return s.commit(c, "DELETED", next, prev), nil
		}
	}
	return s.commit(c, "MODIFIED", next, prev), nil
}

func (s *Server) respond(c *reqCtx, code int, obj map[string]any) error {
	writeJSON(c.w, code, "application/json", obj)
	c.line["status"] = code
	return nil
}

func (s *Server) create(c *reqCtx) error {
	if c.name != "" || c.subresource != "" {
		return s.unimplemented("create on a named path " + c.resource + "/" + c.name + "/" + c.subresource)
	}
	body, err := readBody(c.r)
	if err != nil {
		return err
	}
	fv, err := fieldValidation(c)
	if err != nil {
		return err
	}
	obj, strict, err := s.decodeObject(c, body, fv)
	if err != nil {
		return err
	}
	if s.custom[c.gr] {
		if obj, err = s.decodeCustom(c, obj, fv, nil, strict); err != nil {
			return err
		}
	} else if err := strictOutcome(c, fv, obj, nil, strict); err != nil {
		return err
	}
	if obj["apiVersion"] != c.gv || obj["kind"] != c.api.Kind {
		return apierrors.NewBadRequest(fmt.Sprintf("the API version in the data (%v) does not match the expected API version (%s)", obj["apiVersion"], c.gv))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureNamespace(c); err != nil {
		return err
	}
	md := meta(obj)
	name, _ := md["name"].(string)
	if name == "" {
		if gen, _ := md["generateName"].(string); gen != "" {
			name = gen + s.o.NewUID()[:5]
			md["name"] = name
		}
	}
	if name == "" {
		return apierrors.NewInvalid(schema.GroupKind{Group: c.group, Kind: c.api.Kind}, "", fieldErrors("metadata.name", "Required", "name or generateName is required"))
	}
	if c.ns != "" {
		md["namespace"] = c.ns
	} else {
		delete(md, "namespace")
	}
	if s.bucket(c.gr)[c.ns+"/"+name] != nil {
		return apierrors.NewAlreadyExists(s.gvr(c), name)
	}
	md["uid"] = s.o.NewUID()
	md["creationTimestamp"] = s.now()
	if s.hasGeneration(c) {
		md["generation"] = json.Number("1")
	} else {
		delete(md, "generation")
	}
	delete(md, "resourceVersion")
	if s.hasStatus(c) {
		delete(obj, "status")
	}
	c.line["name"] = name
	if err := s.validateCustomResource(c, obj, nil); err != nil {
		return err
	}
	stored, err := normalize(obj)
	if err != nil {
		return apierrors.NewInternalError(err)
	}
	stored = s.commit(c, "ADDED", stored, nil)
	s.fireRules(c, "create", stored)
	return s.respond(c, http.StatusCreated, stored)
}

func checkName(c *reqCtx, obj map[string]any) error {
	md := meta(obj)
	if name, _ := md["name"].(string); name != c.name {
		return apierrors.NewBadRequest(fmt.Sprintf("the name of the object (%s) does not match the name on the URL (%s)", name, c.name))
	}
	if ns, _ := md["namespace"].(string); ns != "" && ns != c.ns {
		return apierrors.NewBadRequest("the namespace of the provided object does not match the namespace sent on the request")
	}
	return nil
}

func (s *Server) update(c *reqCtx) error {
	body, err := readBody(c.r)
	if err != nil {
		return err
	}
	fv, err := fieldValidation(c)
	if err != nil {
		return err
	}
	obj, strict, err := s.decodeObject(c, body, fv)
	if err != nil {
		return err
	}
	if s.custom[c.gr] {
		if obj, err = s.decodeCustom(c, obj, fv, nil, strict); err != nil {
			return err
		}
	} else if err := strictOutcome(c, fv, obj, nil, strict); err != nil {
		return err
	}
	if err := checkName(c, obj); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, err := s.object(c)
	if err != nil {
		return err
	}
	if want, _ := meta(obj)["resourceVersion"].(string); want != "" && want != meta(prev)["resourceVersion"] {
		return apierrors.NewConflict(s.gvr(c), c.name, errors.New("the object has been modified; please apply your changes to the latest version and try again"))
	}
	if c.ns != "" {
		meta(obj)["namespace"] = c.ns
	}
	next := s.restrict(c, prev, obj)
	if err := s.validateCustomResource(c, next, prev); err != nil {
		return err
	}
	out, err := s.write(c, prev, next)
	if err != nil {
		return err
	}
	if c.subresource == "" {
		s.fireRules(c, "update", out)
	}
	return s.respond(c, http.StatusOK, out)
}

// patchTypes are the media types the apiserver accepts for a PATCH (k8s.io/apiserver
// pkg/endpoints/handlers/patch.go): strategic merge only for built-in kinds; CBOR apply only behind
// an alpha gate, which is off.
func (s *Server) patchTypes(c *reqCtx) []string {
	if s.custom[c.gr] {
		return []string{"application/json-patch+json", "application/merge-patch+json", "application/apply-patch+yaml"}
	}
	return []string{"application/json-patch+json", "application/merge-patch+json", "application/strategic-merge-patch+json", "application/apply-patch+yaml"}
}

func (s *Server) patch(c *reqCtx) error {
	mt := mediaType(c.r)
	accepted := s.patchTypes(c)
	if !hasString(accepted, mt) {
		return unsupportedMediaType(accepted)
	}
	switch mt {
	case "application/apply-patch+yaml":
		if c.r.URL.Query().Get("fieldManager") == "" {
			return apierrors.NewInvalid(schema.GroupKind{Group: "meta.k8s.io", Kind: "PatchOptions"}, "", fieldErrors("fieldManager", "Required", "is required for apply patch"))
		}
		return s.unimplemented("server-side apply (apply-patch with a fieldManager)")
	case "application/strategic-merge-patch+json":
		return s.unimplemented("strategic merge patch on " + c.gr)
	}
	body, err := readBody(c.r)
	if err != nil {
		return err
	}
	fv, err := fieldValidation(c)
	if err != nil {
		return err
	}
	strict, err := patchStrictErrors(fv, mt, body)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, err := s.object(c)
	if err != nil {
		return err
	}
	current, err := json.Marshal(prev)
	if err != nil {
		return apierrors.NewInternalError(err)
	}
	var patched []byte
	switch mt {
	case "application/merge-patch+json":
		patched, err = jsonpatch.MergePatch(current, body)
		if err != nil {
			if errors.Is(err, jsonpatch.ErrBadJSONPatch) || errors.Is(err, jsonpatch.ErrBadJSONDoc) {
				return apierrors.NewBadRequest(err.Error())
			}
			return apierrors.NewInternalError(err)
		}
	default: // application/json-patch+json
		p, err := jsonpatch.DecodePatch(body)
		if err != nil {
			return apierrors.NewBadRequest(err.Error())
		}
		if len(p) > maxJSONPatchOps {
			return apierrors.NewRequestEntityTooLargeError(fmt.Sprintf("The allowed maximum operations in a JSON patch is %d, got %d", maxJSONPatchOps, len(p)))
		}
		if patched, err = p.Apply(current); err != nil {
			return apierrors.NewGenericServerResponse(http.StatusUnprocessableEntity, "", schema.GroupResource{}, "", err.Error(), 0, false)
		}
	}
	next, err := decodeJSONObject(patched)
	if err != nil {
		return apierrors.NewBadRequest(err.Error())
	}
	if next["apiVersion"] != prev["apiVersion"] || next["kind"] != prev["kind"] {
		return apierrors.NewBadRequest("the patch changes the object's apiVersion or kind")
	}
	if err := checkName(c, next); err != nil {
		return err
	}
	// The patched object is decoded as the handler's codec does (patch.go:336-366): a built-in kind
	// into its typed form, a custom resource through its schema; strict errors per fieldValidation.
	switch {
	case s.typed(c):
		obj, decodeStrict, err := decodeBuiltin("application/json", patched, fv)
		if err != nil {
			return patchDecodeError(patched, err)
		}
		if err := strictOutcome(c, fv, obj, patched, append(strict, decodeStrict...)); err != nil {
			return err
		}
		next = obj
	case s.custom[c.gr]:
		if next, err = s.decodeCustom(c, next, fv, patched, strict); err != nil {
			return err
		}
	default:
		if err := strictOutcome(c, fv, next, patched, strict); err != nil {
			return err
		}
	}
	next = s.restrict(c, prev, next)
	if err := s.validateCustomResource(c, next, prev); err != nil {
		return err
	}
	out, err := s.write(c, prev, next)
	if err != nil {
		return err
	}
	if c.subresource == "" {
		s.fireRules(c, "patch", out)
	}
	return s.respond(c, http.StatusOK, out)
}

func (s *Server) remove(c *reqCtx) error {
	if c.subresource != "" {
		return s.unimplemented("delete on a subresource")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, err := s.object(c)
	if err != nil {
		return err
	}
	if fin, _ := meta(prev)["finalizers"].([]any); len(fin) > 0 {
		next := deepCopy(prev).(map[string]any)
		if _, ok := meta(next)["deletionTimestamp"]; !ok {
			meta(next)["deletionTimestamp"] = s.now()
			meta(next)["deletionGracePeriodSeconds"] = json.Number("0")
			// The first deletion bumps a set generation (pkg/registry/rest/delete.go:165-172).
			if g, err := strconv.ParseInt(fmt.Sprint(meta(next)["generation"]), 10, 64); err == nil && g > 0 {
				meta(next)["generation"] = json.Number(strconv.FormatInt(g+1, 10))
			}
		}
		out, err := s.write(c, prev, next)
		if err != nil {
			return err
		}
		s.fireRules(c, "delete", prev)
		return s.respond(c, http.StatusOK, out)
	}
	gone := deepCopy(prev).(map[string]any)
	s.commit(c, "DELETED", gone, prev)
	s.fireRules(c, "delete", prev)
	return s.respond(c, http.StatusOK, gone)
}
