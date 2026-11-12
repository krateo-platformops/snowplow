//go:build js && wasm

package fakeapiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	structuraldefaulting "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	schemaobjectmeta "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/objectmeta"
	structuralpruning "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
)

// A custom resource's write goes through its CRD's schema with apiextensions-apiserver's own code,
// as kube-apiserver does it (k8s.io/apiextensions-apiserver v0.36.5):
//
//	pkg/apiserver/customresource_handler.go:1320-1345, unstructuredSchemaCoercer.apply: object
//	  metadata coercion, pruning of unknown fields (with their paths, for fieldValidation), and
//	  nulls without defaults dropped;
//	pkg/apiserver/schema/defaulting: the schema's defaults;
//	pkg/apiserver/validation: the OpenAPI schema validation (types, enums, required, formats);
//	pkg/apiserver/schema/cel: x-kubernetes-validations, within the apiserver's cost budget.
//
// fieldValidation is the request option (k8s.io/apiserver pkg/endpoints/handlers/create.go:
// 125-140): Strict refuses unknown fields with 400 "strict decoding error: ...", Warn (the default)
// answers them as Warning headers, Ignore drops them silently.

type crdVersion struct {
	structural *structuralschema.Structural
	validator  apiservervalidation.SchemaValidator
	cel        *cel.Validator
}

type crdInfo struct {
	group, kind           string
	preserveUnknownFields bool
	versions              map[string]*crdVersion
}

// loadCRD reads one CustomResourceDefinition's per-version schemas.
func loadCRD(obj map[string]any) (string, *crdInfo, error) {
	u, err := toUnstructured(obj)
	if err != nil {
		return "", nil, err
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u, crd); err != nil {
		return "", nil, err
	}
	info := &crdInfo{group: crd.Spec.Group, kind: crd.Spec.Names.Kind, preserveUnknownFields: crd.Spec.PreserveUnknownFields, versions: map[string]*crdVersion{}}
	for _, v := range crd.Spec.Versions {
		if v.Schema == nil || v.Schema.OpenAPIV3Schema == nil {
			continue
		}
		internal := &apiextensions.CustomResourceValidation{}
		if err := apiextensionsv1.Convert_v1_CustomResourceValidation_To_apiextensions_CustomResourceValidation(v.Schema, internal, nil); err != nil {
			return "", nil, fmt.Errorf("CRD %s version %s: %w", crd.Name, v.Name, err)
		}
		st, err := structuralschema.NewStructural(internal.OpenAPIV3Schema)
		if err != nil {
			return "", nil, fmt.Errorf("CRD %s version %s: not a structural schema: %w", crd.Name, v.Name, err)
		}
		validator, _, err := apiservervalidation.NewSchemaValidator(internal.OpenAPIV3Schema)
		if err != nil {
			return "", nil, fmt.Errorf("CRD %s version %s: %w", crd.Name, v.Name, err)
		}
		info.versions[v.Name] = &crdVersion{structural: st, validator: validator, cel: cel.NewValidator(st, true, celconfig.PerCallLimit)}
	}
	return crd.Spec.Group + "/" + crd.Spec.Names.Plural, info, nil
}

// toUnstructured is obj with numbers as the apiserver's unstructured content holds them (int64,
// float64), which the schema code expects.
func toUnstructured(obj map[string]any) (map[string]any, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	var u map[string]any
	if err := utiljson.Unmarshal(b, &u); err != nil {
		return nil, err
	}
	return u, nil
}

// fieldValidation is the request's option; "" is the apiserver's default, Warn.
func fieldValidation(c *reqCtx, q url.Values) (string, error) {
	v := q.Get("fieldValidation")
	switch v {
	case "":
		return "Warn", nil
	case "Ignore", "Warn", "Strict":
		return v, nil
	}
	kind := map[string]string{"create": "CreateOptions", "update": "UpdateOptions", "patch": "PatchOptions"}[c.verb]
	return "", apierrors.NewInvalid(schema.GroupKind{Group: "meta.k8s.io", Kind: kind}, "", field.ErrorList{field.NotSupported(field.NewPath("fieldValidation"), v, []string{"Ignore", "Strict", "Warn"})})
}

// admitCustomResource prunes, defaults and validates a custom resource's write against its CRD's
// schema for the request's version. old is the stored object for an update or patch, nil for a
// create. It returns the object to store.
func (s *Server) admitCustomResource(c *reqCtx, obj, old map[string]any) (map[string]any, error) {
	info := s.crds[c.gr]
	if info == nil {
		return obj, nil
	}
	ver := info.versions[c.version]
	if ver == nil {
		return obj, nil
	}
	fv := "Ignore" // a controller's write (applyStatus) has no request options
	if c.r != nil {
		var err error
		if fv, err = fieldValidation(c, c.r.URL.Query()); err != nil {
			return nil, err
		}
	}
	u, err := toUnstructured(obj)
	if err != nil {
		return nil, apierrors.NewInternalError(err)
	}
	kind, apiVersion := u["kind"], u["apiVersion"]
	track := fv != "Ignore"
	objectMeta, foundMeta, unknown, err := schemaobjectmeta.GetObjectMetaWithOptions(u, schemaobjectmeta.ObjectMetaOptions{DropMalformedFields: true, ReturnUnknownFieldPaths: track})
	if err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}
	if !info.preserveUnknownFields {
		unknown = append(unknown, structuralpruning.PruneWithOptions(u, ver.structural, true, structuralschema.UnknownFieldPathOptions{TrackUnknownFieldPaths: track})...)
		structuraldefaulting.PruneNonNullableNullsWithoutDefaults(u, ver.structural)
	}
	cerr, paths := schemaobjectmeta.CoerceWithOptions(nil, u, ver.structural, false, schemaobjectmeta.CoerceOptions{DropInvalidFields: true, ReturnUnknownFieldPaths: track})
	if cerr != nil {
		return nil, apierrors.NewInvalid(schema.GroupKind{Group: info.group, Kind: info.kind}, c.name, field.ErrorList{cerr})
	}
	unknown = append(unknown, paths...)
	u["kind"], u["apiVersion"] = kind, apiVersion
	if foundMeta {
		if err := schemaobjectmeta.SetObjectMeta(u, objectMeta); err != nil {
			return nil, apierrors.NewInternalError(err)
		}
	}
	if len(unknown) > 0 {
		msgs := make([]string, len(unknown))
		for i, p := range unknown {
			msgs[i] = fmt.Sprintf("unknown field %q", p)
		}
		switch fv {
		case "Strict":
			return nil, apierrors.NewBadRequest("strict decoding error: " + strings.Join(msgs, ", "))
		case "Warn":
			for _, m := range msgs {
				if c.w == nil {
					break
				}
				c.w.Header().Add("Warning", "299 - "+strconv.Quote(m))
			}
		}
	}
	structuraldefaulting.Default(u, ver.structural)

	gk := schema.GroupKind{Group: info.group, Kind: info.kind}
	var errs field.ErrorList
	var oldU map[string]any
	if old != nil {
		if oldU, err = toUnstructured(old); err != nil {
			return nil, apierrors.NewInternalError(err)
		}
		errs = apiservervalidation.ValidateCustomResourceUpdate(nil, u, oldU, ver.validator)
	} else {
		errs = apiservervalidation.ValidateCustomResource(nil, u, ver.validator)
	}
	if ver.cel != nil {
		var oldObj any
		if oldU != nil {
			oldObj = oldU
		}
		celErrs, _ := ver.cel.Validate(context.Background(), nil, ver.structural, u, oldObj, celconfig.RuntimeCELCostBudget)
		errs = append(errs, celErrs...)
	}
	if len(errs) > 0 {
		return nil, apierrors.NewInvalid(gk, c.name, errs)
	}
	return normalize(u)
}
