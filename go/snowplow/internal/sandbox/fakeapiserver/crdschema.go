//go:build js && wasm

package fakeapiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/model"
	structuraldefaulting "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	structurallisttype "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/listtype"
	schemaobjectmeta "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/objectmeta"
	structuralpruning "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	apiextensionsfeatures "k8s.io/apiextensions-apiserver/pkg/features"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/cel/common"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// A custom resource's write goes through its CRD with apiextensions-apiserver's own code, in the
// order kube-apiserver runs it (k8s.io/apiextensions-apiserver v0.36.5):
//
//  1. decoding (coerceCustomResource): pkg/apiserver/customresource_handler.go, the request
//     decoder (unstructuredNegotiatedSerializer.DecoderToVersion, :1189-1200) and
//     unstructuredSchemaCoercer.apply (:1406-1470): object metadata read and coerced, unknown
//     fields pruned (their paths returned when fieldValidation is Warn or Strict), nulls without
//     defaults dropped; then the schema's defaults (unstructuredDefaulter). A malformed object is
//     a decoding error.
//  2. validation (validateCustomResource): pkg/registry/customresource strategy.go Validate and
//     ValidateUpdate, status_strategy.go ValidateUpdate for /status, and validator.go. The
//     registry package itself is not imported: it compiles in the generic registry's etcd3
//     storage, which a browser build has no use for. Every check it makes is called here from the
//     same exported functions; the three it keeps unexported (hasBlockingErr, ValidateTypeMeta,
//     the scale checks) are transcribed below with their line numbers.
//
// The registry's ratcheting time metric (validation.Metrics.ObserveRatchetingTime) is not kept.

type crdVersion struct {
	kind       schema.GroupVersionKind
	structural *structuralschema.Structural
	// validator is the whole schema's; statusValidator is the status property's, set when the
	// version has a status subresource (customresource_handler.go:815-833).
	validator, statusValidator apiservervalidation.SchemaValidator
	cel                        *cel.Validator
	scale                      *apiextensions.CustomResourceSubresourceScale
}

type crdInfo struct {
	group, kind           string
	namespaced            bool
	preserveUnknownFields bool
	versions              map[string]*crdVersion
}

// loadCRD reads one CustomResourceDefinition's per-version schemas and subresources, as
// customresource_handler.go:795-870 builds each version's strategy.
func loadCRD(obj map[string]any) (string, *crdInfo, error) {
	u, err := toUnstructured(obj)
	if err != nil {
		return "", nil, err
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u, crd); err != nil {
		return "", nil, err
	}
	info := &crdInfo{group: crd.Spec.Group, kind: crd.Spec.Names.Kind, namespaced: crd.Spec.Scope == apiextensionsv1.NamespaceScoped,
		preserveUnknownFields: crd.Spec.PreserveUnknownFields, versions: map[string]*crdVersion{}}
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
		ver := &crdVersion{kind: schema.GroupVersionKind{Group: crd.Spec.Group, Version: v.Name, Kind: crd.Spec.Names.Kind},
			structural: st, validator: validator, cel: cel.NewValidator(st, true, celconfig.PerCallLimit)}
		if v.Subresources != nil && v.Subresources.Status != nil && internal.OpenAPIV3Schema.Properties != nil {
			// "for the status subresource, validate only against the status schema"
			if statusSchema, ok := internal.OpenAPIV3Schema.Properties["status"]; ok {
				if ver.statusValidator, _, err = apiservervalidation.NewSchemaValidator(&statusSchema); err != nil {
					return "", nil, fmt.Errorf("CRD %s version %s: %w", crd.Name, v.Name, err)
				}
			}
		}
		if v.Subresources != nil && v.Subresources.Scale != nil {
			ver.scale = &apiextensions.CustomResourceSubresourceScale{}
			if err := apiextensionsv1.Convert_v1_CustomResourceSubresourceScale_To_apiextensions_CustomResourceSubresourceScale(v.Subresources.Scale, ver.scale, nil); err != nil {
				return "", nil, fmt.Errorf("CRD %s version %s: %w", crd.Name, v.Name, err)
			}
		}
		info.versions[v.Name] = ver
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

// crdVersionOf is the request's custom resource version, or nil (not a custom resource, or a
// version without a schema).
func (s *Server) crdVersionOf(c *reqCtx) (*crdInfo, *crdVersion) {
	info := s.crds[c.gr]
	if info == nil {
		return nil, nil
	}
	return info, info.versions[c.version]
}

// coerceCustomResource is step 1: the decoded object as the apiserver's request decoder leaves it,
// and the unknown field paths, as strict decoding errors, when fv is Warn or Strict. Any error is a
// decoding error (the caller answers it as the handler does: decodeError).
func (s *Server) coerceCustomResource(c *reqCtx, obj map[string]any, fv string) (map[string]any, []error, error) {
	info, ver := s.crdVersionOf(c)
	if ver == nil {
		return obj, nil, nil
	}
	u, err := toUnstructured(obj)
	if err != nil {
		return nil, nil, err
	}
	track := fv == "Warn" || fv == "Strict"
	kind, apiVersion := u["kind"], u["apiVersion"]
	// The request decoder does not drop malformed metadata (dropInvalidMetadata is false there).
	objectMeta, foundMeta, unknown, err := schemaobjectmeta.GetObjectMetaWithOptions(u, schemaobjectmeta.ObjectMetaOptions{ReturnUnknownFieldPaths: track})
	if err != nil {
		return nil, nil, err
	}
	if gv, _ := schema.ParseGroupVersion(fmt.Sprint(apiVersion)); gv.Group == info.group && kind == info.kind {
		if !info.preserveUnknownFields {
			unknown = append(unknown, structuralpruning.PruneWithOptions(u, ver.structural, true, structuralschema.UnknownFieldPathOptions{TrackUnknownFieldPaths: track})...)
			structuraldefaulting.PruneNonNullableNullsWithoutDefaults(u, ver.structural)
		}
		cerr, paths := schemaobjectmeta.CoerceWithOptions(nil, u, ver.structural, false, schemaobjectmeta.CoerceOptions{ReturnUnknownFieldPaths: track})
		if cerr != nil {
			return nil, nil, cerr
		}
		unknown = append(unknown, paths...)
	}
	u["kind"], u["apiVersion"] = kind, apiVersion
	if foundMeta {
		if err := schemaobjectmeta.SetObjectMeta(u, objectMeta); err != nil {
			return nil, nil, err
		}
	}
	structuraldefaulting.Default(u, ver.structural)
	var strict []error
	if track {
		for _, p := range unknown {
			strict = append(strict, fmt.Errorf(`unknown field "%s"`, p))
		}
	}
	out, err := normalize(u)
	return out, strict, err
}

// validateCustomResource is step 2, the strategy's validation: create (old nil), update, or a
// /status write. It returns the 422, or nil.
func (s *Server) validateCustomResource(c *reqCtx, obj, old map[string]any) error {
	info, ver := s.crdVersionOf(c)
	if ver == nil {
		return nil
	}
	u, err := toUnstructured(obj)
	if err != nil {
		return apierrors.NewInternalError(err)
	}
	uNew := &unstructured.Unstructured{Object: u}
	var errs field.ErrorList
	switch {
	case old == nil:
		errs = ver.validate(info, uNew)
	default:
		o, err := toUnstructured(old)
		if err != nil {
			return apierrors.NewInternalError(err)
		}
		uOld := &unstructured.Unstructured{Object: o}
		if c.subresource == "status" {
			errs = ver.validateStatusUpdate(uNew, uOld)
		} else {
			errs = ver.validateUpdate(uNew, uOld)
		}
	}
	if len(errs) > 0 {
		// Named by the object's own metadata.name, as rest/create.go:130 and update.go:156 do: on a
		// create the URL has no name.
		name, _ := meta(obj)["name"].(string)
		return apierrors.NewInvalid(schema.GroupKind{Group: info.group, Kind: info.kind}, name, errs)
	}
	return nil
}

func ratcheting() bool {
	return utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CRDValidationRatcheting)
}

// validate is strategy.go Validate (:201-228) with validator.go Validate (:46-59).
func (v *crdVersion) validate(info *crdInfo, u *unstructured.Unstructured) field.ErrorList {
	ctx := context.Background()
	var errs field.ErrorList
	if tm := v.validateTypeMeta(u); len(tm) > 0 {
		errs = tm
	} else {
		errs = append(errs, validation.ValidateObjectMetaAccessor(u, info.namespaced, validation.NameIsDNSSubdomain, field.NewPath("metadata"))...)
		errs = append(errs, apiservervalidation.ValidateCustomResource(nil, u.UnstructuredContent(), v.validator)...)
		errs = append(errs, v.validateScaleSpec(u)...)
		errs = append(errs, v.validateScaleStatus(u)...)
	}
	errs = append(errs, schemaobjectmeta.Validate(nil, u.Object, v.structural, false)...)
	errs = append(errs, structurallisttype.ValidateListSetsAndMaps(nil, v.structural, u.Object)...)
	if v.cel != nil {
		if has, err := hasBlockingErr(errs); has {
			errs = append(errs, err)
		} else {
			celErrs, _ := v.cel.Validate(ctx, nil, v.structural, u.Object, nil, celconfig.RuntimeCELCostBudget)
			errs = append(errs, celErrs...)
		}
	}
	return errs
}

// validateUpdate is strategy.go ValidateUpdate (:274-320) with validator.go ValidateUpdate
// (:61-74): ratcheting (CRDValidationRatcheting, GA and locked on since 1.33) lets a field the
// update leaves unchanged keep failing its schema.
func (v *crdVersion) validateUpdate(uNew, uOld *unstructured.Unstructured) field.ErrorList {
	ctx := context.Background()
	var options []apiservervalidation.ValidationOption
	var celOptions []cel.Option
	if ratcheting() {
		correlated := common.NewCorrelatedObject(uNew.Object, uOld.Object, &model.Structural{Structural: v.structural})
		options = append(options, apiservervalidation.WithRatcheting(correlated))
		celOptions = append(celOptions, cel.WithRatcheting(correlated))
	}
	var errs field.ErrorList
	if tm := v.validateTypeMeta(uNew); len(tm) > 0 {
		errs = tm
	} else {
		errs = append(errs, validation.ValidateObjectMetaAccessorUpdate(uNew, uOld, field.NewPath("metadata"))...)
		errs = append(errs, apiservervalidation.ValidateCustomResourceUpdate(nil, uNew.UnstructuredContent(), uOld.UnstructuredContent(), v.validator, options...)...)
		errs = append(errs, v.validateScaleSpec(uNew)...)
		errs = append(errs, v.validateScaleStatus(uNew)...)
	}
	errs = append(errs, schemaobjectmeta.Validate(nil, uNew.Object, v.structural, false)...)
	if oldErrs := structurallisttype.ValidateListSetsAndMaps(nil, v.structural, uOld.Object); len(oldErrs) == 0 {
		errs = append(errs, structurallisttype.ValidateListSetsAndMaps(nil, v.structural, uNew.Object)...)
	}
	if v.cel != nil {
		if has, err := hasBlockingErr(errs); has {
			errs = append(errs, err)
		} else {
			celErrs, _ := v.cel.Validate(ctx, nil, v.structural, uNew.Object, uOld.Object, celconfig.RuntimeCELCostBudget, celOptions...)
			errs = append(errs, celErrs...)
		}
	}
	return errs
}

// validateStatusUpdate is status_strategy.go ValidateUpdate (:94-141) with validator.go
// ValidateStatusUpdate (:99-113): the status schema only, ratcheted from status.
func (v *crdVersion) validateStatusUpdate(uNew, uOld *unstructured.Unstructured) field.ErrorList {
	ctx := context.Background()
	var options []apiservervalidation.ValidationOption
	var celOptions []cel.Option
	if ratcheting() {
		correlated := common.NewCorrelatedObject(uNew.Object, uOld.Object, &model.Structural{Structural: v.structural})
		if v.structural != nil {
			options = append(options, apiservervalidation.WithRatcheting(correlated.Key("status")))
		}
		celOptions = append(celOptions, cel.WithRatcheting(correlated))
	}
	var errs field.ErrorList
	if tm := v.validateTypeMeta(uNew); len(tm) > 0 {
		errs = tm
	} else {
		errs = append(errs, validation.ValidateObjectMetaAccessorUpdate(uNew, uOld, field.NewPath("metadata"))...)
		if status, ok := uNew.UnstructuredContent()["status"]; ok {
			errs = append(errs, apiservervalidation.ValidateCustomResourceUpdate(field.NewPath("status"), status, uOld.UnstructuredContent()["status"], v.statusValidator, options...)...)
		}
		errs = append(errs, v.validateScaleStatus(uNew)...)
	}
	if newErrs := structurallisttype.ValidateListSetsAndMaps(nil, v.structural, uNew.Object); len(newErrs) > 0 {
		if oldErrs := structurallisttype.ValidateListSetsAndMaps(nil, v.structural, uOld.Object); len(oldErrs) == 0 {
			errs = append(errs, newErrs...)
		}
	}
	if v.cel != nil {
		if has, err := hasBlockingErr(errs); has {
			errs = append(errs, err)
		} else {
			celErrs, _ := v.cel.Validate(ctx, nil, v.structural, uNew.Object, uOld.Object, celconfig.RuntimeCELCostBudget, celOptions...)
			errs = append(errs, celErrs...)
		}
	}
	return errs
}

// hasBlockingErr is strategy.go:405-412: errors after which the CEL rules are not run.
func hasBlockingErr(errs field.ErrorList) (bool, *field.Error) {
	for _, err := range errs {
		if err.Type == field.ErrorTypeNotSupported || err.Type == field.ErrorTypeRequired || err.Type == field.ErrorTypeTooLong || err.Type == field.ErrorTypeTooMany || err.Type == field.ErrorTypeTypeInvalid {
			return true, field.Invalid(nil, nil, "some validation rules were not checked because the object was invalid; correct the existing errors to complete validation")
		}
	}
	return false, nil
}

// validateTypeMeta is validator.go ValidateTypeMeta (:115-129).
func (v *crdVersion) validateTypeMeta(obj *unstructured.Unstructured) field.ErrorList {
	typeAccessor, err := apimeta.TypeAccessor(obj)
	if err != nil {
		return field.ErrorList{field.Invalid(field.NewPath("kind"), nil, err.Error())}
	}
	var errs field.ErrorList
	if typeAccessor.GetKind() != v.kind.Kind {
		errs = append(errs, field.Invalid(field.NewPath("kind"), typeAccessor.GetKind(), fmt.Sprintf("must be %v", v.kind.Kind)))
	}
	if typeAccessor.GetAPIVersion() != v.kind.Group+"/"+v.kind.Version {
		errs = append(errs, field.Invalid(field.NewPath("apiVersion"), typeAccessor.GetAPIVersion(), fmt.Sprintf("must be %v", v.kind.Group+"/"+v.kind.Version)))
	}
	return errs
}

// validateScaleSpec is validator.go ValidateScaleSpec (:131-150).
func (v *crdVersion) validateScaleSpec(obj *unstructured.Unstructured) field.ErrorList {
	if v.scale == nil {
		return nil
	}
	var errs field.ErrorList
	specReplicasPath := strings.TrimPrefix(v.scale.SpecReplicasPath, ".")
	specReplicas, _, err := unstructured.NestedInt64(obj.UnstructuredContent(), strings.Split(specReplicasPath, ".")...)
	if err != nil {
		errs = append(errs, field.Invalid(field.NewPath(v.scale.SpecReplicasPath), specReplicas, err.Error()))
	} else if specReplicas < 0 {
		errs = append(errs, field.Invalid(field.NewPath(v.scale.SpecReplicasPath), specReplicas, "should be a non-negative integer"))
	} else if specReplicas > math.MaxInt32 {
		errs = append(errs, field.Invalid(field.NewPath(v.scale.SpecReplicasPath), specReplicas, fmt.Sprintf("should be less than or equal to %v", math.MaxInt32)))
	}
	return errs
}

// validateScaleStatus is validator.go ValidateScaleStatus (:152-180).
func (v *crdVersion) validateScaleStatus(obj *unstructured.Unstructured) field.ErrorList {
	if v.scale == nil {
		return nil
	}
	var errs field.ErrorList
	statusReplicasPath := strings.TrimPrefix(v.scale.StatusReplicasPath, ".")
	statusReplicas, _, err := unstructured.NestedInt64(obj.UnstructuredContent(), strings.Split(statusReplicasPath, ".")...)
	if err != nil {
		errs = append(errs, field.Invalid(field.NewPath(v.scale.StatusReplicasPath), statusReplicas, err.Error()))
	} else if statusReplicas < 0 {
		errs = append(errs, field.Invalid(field.NewPath(v.scale.StatusReplicasPath), statusReplicas, "should be a non-negative integer"))
	} else if statusReplicas > math.MaxInt32 {
		errs = append(errs, field.Invalid(field.NewPath(v.scale.StatusReplicasPath), statusReplicas, fmt.Sprintf("should be less than or equal to %v", math.MaxInt32)))
	}
	if v.scale.LabelSelectorPath != nil {
		labelSelectorPath := strings.TrimPrefix(*v.scale.LabelSelectorPath, ".")
		labelSelector, _, err := unstructured.NestedString(obj.UnstructuredContent(), strings.Split(labelSelectorPath, ".")...)
		if err != nil {
			errs = append(errs, field.Invalid(field.NewPath(*v.scale.LabelSelectorPath), labelSelector, err.Error()))
		}
	}
	return errs
}
