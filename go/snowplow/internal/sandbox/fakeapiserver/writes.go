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

// decodeObject reads a create or update body. A built-in kind goes through client-go's scheme, JSON
// or protobuf, and is stored in its typed form's JSON (canonical, as the apiserver stores it); a
// custom resource is JSON. A protobuf body of a kind the scheme does not register is 415.
func (s *Server) decodeObject(c *reqCtx, body []byte) (map[string]any, error) {
	mt := mediaType(c.r)
	if mt == "" {
		mt = "application/json"
	}
	switch mt {
	case "application/json", "application/yaml", "application/vnd.kubernetes.protobuf":
	default:
		return nil, unsupportedMediaType(s.bodyTypes(c))
	}
	builtin := !s.custom[c.gr]
	if mt == "application/vnd.kubernetes.protobuf" && !builtin {
		return nil, unsupportedMediaType(s.bodyTypes(c))
	}
	if builtin {
		obj, gvk, err := scheme.Codecs.UniversalDeserializer().Decode(body, nil, nil)
		if err != nil {
			if mt == "application/vnd.kubernetes.protobuf" {
				return nil, unsupportedMediaType(s.bodyTypes(c))
			}
			return nil, apierrors.NewBadRequest(fmt.Sprintf("the object provided is unrecognized: %v", err))
		}
		u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if err != nil {
			return nil, apierrors.NewInternalError(err)
		}
		u["apiVersion"], u["kind"] = gvk.GroupVersion().String(), gvk.Kind
		return normalize(u)
	}
	if mt == "application/yaml" {
		return nil, s.unimplemented("a YAML body for a custom resource")
	}
	obj, err := decodeJSONObject(body)
	if err != nil {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("the object provided is unrecognized: %v", err))
	}
	return obj, nil
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
func (s *Server) commit(c *reqCtx, typ string, obj, prev map[string]any) map[string]any {
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

// bumpGeneration: metadata.generation increments when anything but metadata (and, with a status
// subresource, status) changes.
func (s *Server) bumpGeneration(c *reqCtx, prev, next map[string]any) {
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
	obj, err := s.decodeObject(c, body)
	if err != nil {
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
	md["generation"] = json.Number("1")
	delete(md, "resourceVersion")
	if s.hasStatus(c) {
		delete(obj, "status")
	}
	c.line["name"] = name
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
	obj, err := s.decodeObject(c, body)
	if err != nil {
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
	out, err := s.write(c, prev, s.restrict(c, prev, obj))
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
		return apierrors.NewGenericServerResponse(http.StatusUnsupportedMediaType, "", schema.GroupResource{}, "",
			"the body of the request was in an unknown format - accepted media types include: "+strings.Join(accepted, ", "), 0, false)
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
	if !s.custom[c.gr] {
		// A built-in kind is stored in its typed form, as after a create or update.
		b, _ := json.Marshal(next)
		if obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(b, nil, nil); err == nil {
			if u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj); err == nil {
				u["apiVersion"], u["kind"] = next["apiVersion"], next["kind"]
				next = u
			}
		} else {
			return apierrors.NewBadRequest(err.Error())
		}
	}
	out, err := s.write(c, prev, s.restrict(c, prev, next))
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
