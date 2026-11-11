// preview.go — krateo.audit.preview (schema v1, installer#716 §2.4, #1291):
// whether a record's Kubernetes target is in the preview sandbox.
//
// THE RULE (the shared schema's): true when the target is a namespaced object
// whose Namespace carries the label krateo.io/purpose=preview-sandbox, as READ
// FROM APISERVER STATE by the writer; false for any other Kubernetes target, a
// cluster-scoped one included. Never read from the object itself: whoever
// creates an object writes its labels. When the Namespace could not be read,
// the flag is left unknown and the shared writer marks the record preview: a
// gap is recorded, never guessed. A record that names only an external
// endpoint carries no flag.
//
// WHERE THE NAMESPACE IS READ. snowplow's own ServiceAccount (its grant reads
// every resource) keeps a metadata-only informer on namespaces
// (StartNamespaceReader), so a record reads the label from memory. A
// Namespace the informer has not seen yet (it is not synced, or the Namespace
// was created a moment ago) is read with one GET on the same client: the
// authority's answer, never the informer's absence, decides. A NotFound is an
// answer (no such Namespace, so no label): false. Any other error leaves the
// flag unknown.
package audit

import (
	"context"
	"sync/atomic"

	krateoaudit "github.com/krateo-platformops/audit"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"
	"k8s.io/client-go/tools/cache"
)

// The Namespace label that puts a namespace in the preview sandbox (the shared
// schema's krateo.audit.preview).
const (
	PreviewLabel        = "krateo.io/purpose"
	PreviewLabelSandbox = "preview-sandbox"
)

// NamespaceReader reads a Namespace's labels from apiserver state. found is
// false, with a nil error, when the apiserver answers that the Namespace does
// not exist.
type NamespaceReader interface {
	NamespaceLabels(ctx context.Context, name string) (labels map[string]string, found bool, err error)
}

type namespaceReaderBox struct{ r NamespaceReader }

var namespaces atomic.Pointer[namespaceReaderBox]

// SetNamespaceReader installs the process-wide Namespace reader the preview
// flag is read through (main, StartNamespaceReader). With none installed, a
// namespaced target's flag is unknown, and its record is marked preview.
func SetNamespaceReader(r NamespaceReader) {
	if r == nil {
		namespaces.Store(nil)
		return
	}
	namespaces.Store(&namespaceReaderBox{r: r})
}

// previewOf is the preview flag of a Kubernetes target in namespace ("" for a
// target that is in no namespace).
func previewOf(ctx context.Context, namespace string) krateoaudit.Preview {
	if namespace == "" {
		return krateoaudit.PreviewFalse
	}
	b := namespaces.Load()
	if b == nil {
		return krateoaudit.PreviewUnknown
	}
	labels, found, err := b.r.NamespaceLabels(ctx, namespace)
	switch {
	case err != nil:
		return krateoaudit.PreviewUnknown
	case !found:
		return krateoaudit.PreviewFalse
	}
	return krateoaudit.PreviewOf(labels[PreviewLabel] == PreviewLabelSandbox)
}

var namespacesGVR = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}

// informerNamespaceReader reads a Namespace from a metadata informer, and from
// the apiserver when the informer does not hold it.
type informerNamespaceReader struct {
	client metadata.Interface
	lister cache.GenericLister
	synced cache.InformerSynced
}

// StartNamespaceReader starts a metadata-only informer on namespaces with
// client (snowplow's own ServiceAccount) until ctx is done, and returns the
// reader over it. It does not wait for the informer to sync: until it has, a
// record's Namespace is read with a GET.
func StartNamespaceReader(ctx context.Context, client metadata.Interface) NamespaceReader {
	f := metadatainformer.NewSharedInformerFactory(client, 0)
	gi := f.ForResource(namespacesGVR)
	r := &informerNamespaceReader{client: client, lister: gi.Lister(), synced: gi.Informer().HasSynced}
	f.Start(ctx.Done())
	return r
}

func (r *informerNamespaceReader) NamespaceLabels(ctx context.Context, name string) (map[string]string, bool, error) {
	if r.synced() {
		if obj, err := r.lister.Get(name); err == nil {
			if m, ok := obj.(*metav1.PartialObjectMetadata); ok {
				return m.Labels, true, nil
			}
		}
	}
	m, err := r.client.Resource(namespacesGVR).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return m.Labels, true, nil
}
