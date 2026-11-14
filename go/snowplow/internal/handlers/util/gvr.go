package util

import (
	"fmt"
	"github.com/krateo-platformops/snowplow/internal/apipath"
	"net/http"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func ParseGVR(req *http.Request) (gvr schema.GroupVersionResource, err error) {
	api := req.URL.Query().Get("apiVersion")
	if len(api) == 0 {
		err = fmt.Errorf("missing 'apiVersion' query parameter")
		return
	}

	res := req.URL.Query().Get("resource")
	if len(res) == 0 {
		err = fmt.Errorf("missing 'resource' query parameter")
		return
	}

	gv, err := schema.ParseGroupVersion(api)
	if err != nil {
		return gvr, err
	}

	gvr = gv.WithResource(res)
	// #1257 / #1414 — the group, the version and the resource each become one
	// segment of an apiserver path, on every route that reads this query. A
	// value that is not one segment is refused here, before any discovery or
	// dial (apipath.Segment is the one rule).
	if err := apipath.CheckGVR(gvr, apipath.Query); err != nil {
		return schema.GroupVersionResource{}, err
	}
	return
}
