package dynamic

// discovery_document_names_1426_test.go (#1426 F1): on a mapper miss the read's path is built
// from the name the group/version's own discovery document gives for the resource. That name is
// the apiserver's, but it is still a value that becomes a path segment, and the document is
// matched by this client, not by the mapper. Three things must hold:
//
//   - a document name that is not one path segment is refused (the document is not usable): the
//     read fails, nothing is dialled, and it is not "not served";
//   - a plural match anywhere in the document wins over a singular match, as in the mapper;
//   - names are compared without regard to case, and the path carries the document's own name.
//
// A CRD's or a built-in's document cannot produce these shapes (their names are validated and
// unique per group); an aggregated API's can. The apiserver here answers every object path, so a
// read that went to the wrong place would succeed and be seen.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/apipath"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// oddDocumentApiserver serves an aggregated root discovery that lists the group with one
// unrelated resource (so the mapper has no mapping for anything asked here), and the
// group/version's own document with the entries under test.
type oddDocumentApiserver struct {
	entries string // the JSON entries of /apis/g.example.org/v1's "resources"

	mu   sync.Mutex
	seen []string
}

func (a *oddDocumentApiserver) objectPaths() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for _, p := range a.seen {
		if p != "/api" && p != "/apis" && p != "/apis/g.example.org/v1" {
			out = append(out, p)
		}
	}
	return out
}

func (a *oddDocumentApiserver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.seen = append(a.seen, r.URL.Path)
	a.mu.Unlock()

	const aggregated = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList"
	verbs := `["get","list"]`
	switch r.URL.Path {
	case "/api":
		w.Header().Set("Content-Type", aggregated)
		_, _ = io.WriteString(w, `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[`+
			`{"metadata":{"creationTimestamp":null},"versions":[{"version":"v1","resources":[],"freshness":"Current"}]}]}`)
	case "/apis":
		w.Header().Set("Content-Type", aggregated)
		_, _ = io.WriteString(w, `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[`+
			`{"metadata":{"name":"g.example.org","creationTimestamp":null},"versions":[{"version":"v1","resources":[`+
			`{"resource":"placeholders","responseKind":{"group":"g.example.org","version":"v1","kind":"Placeholder"},"scope":"Namespaced","singularResource":"placeholder","verbs":`+verbs+`}`+
			`],"freshness":"Current"}]}]}`)
	case "/apis/g.example.org/v1":
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"g.example.org/v1","resources":[`+a.entries+`]}`)
	default:
		// Every other path answers an object named after the path's last-but-one segment, so
		// the test reads which collection was dialled.
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		collection := ""
		if len(parts) >= 2 {
			collection = parts[len(parts)-2]
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"g.example.org/v1","kind":"Thing","metadata":{"name":"x","namespace":"ns","labels":{"collection":"`+collection+`"}}}`)
	}
}

func docEntry(name, singular string) string {
	return `{"name":"` + name + `","singularName":"` + singular + `","namespaced":true,"kind":"Thing","verbs":["get","list"]}`
}

func TestDiscoveryDocument_ANameThatIsNotOnePathSegmentIsRefused(t *testing.T) {
	for _, c := range []struct{ name, entries, ask string }{
		{"dot_dot_matched_by_its_singular", docEntry("..", "foo"), "foo"},
		{"dot_matched_by_its_singular", docEntry(".", "foo"), "foo"},
		{"percent_matched_by_its_singular", docEntry("foos%2Fx", "foo"), "foo"},
		{"percent_matched_by_its_plural", docEntry("foos%2Fx", "foo"), "foos%2Fx"},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := &oddDocumentApiserver{entries: c.entries}
			cli := newClient1414(t, a)
			opts := Options{Namespace: "ns", GVR: schema.GroupVersionResource{Group: "g.example.org", Version: "v1", Resource: c.ask}}
			if c.ask != "foo" {
				// The caller's own value is refused first when it is not a segment; this row
				// is about the document's, so it goes straight to the document.
				uc := cli.(*unstructuredClient)
				_, err := uc.servedResource(opts.GVR)
				assertDocumentUnusable(t, err)
				return
			}
			_, err := cli.Get(context.Background(), "x", opts)
			assertDocumentUnusable(t, err)
			if _, err = cli.List(context.Background(), opts); err == nil {
				t.Errorf("List succeeded on an unusable document")
			}
			if got := a.objectPaths(); len(got) != 0 {
				t.Fatalf("nothing may be dialled from an unusable document, the apiserver saw %v", got)
			}
		})
	}
}

func assertDocumentUnusable(t *testing.T, err error) {
	t.Helper()
	var bad *apipath.BadSegmentError
	if !errors.As(err, &bad) {
		t.Fatalf("want the document's name refused (BadSegmentError), got %T: %v", err, err)
	}
	if IsNotServed(err) {
		t.Fatalf("an unusable document is not \"not served\": %v", err)
	}
	if !strings.HasPrefix(bad.Error(), `invalid discovery document resource "name": `) {
		t.Errorf("the refusal must name the document's field, got %q", bad.Error())
	}
}

func TestDiscoveryDocument_APluralMatchWinsOverASingularMatch(t *testing.T) {
	for _, c := range []struct{ name, entries, ask, want string }{
		// `foo` is both the singular of `foos` and the plural of another resource.
		{"singular_entry_first", docEntry("foos", "foo") + "," + docEntry("foo", "fo"), "foo", "foo"},
		{"plural_entry_first", docEntry("foo", "fo") + "," + docEntry("foos", "foo"), "foo", "foo"},
		{"only_a_singular_matches", docEntry("foos", "foo") + "," + docEntry("bars", "bar"), "foo", "foos"},
		{"the_plural_itself", docEntry("foos", "foo") + "," + docEntry("foo", "fo"), "foos", "foos"},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := &oddDocumentApiserver{entries: c.entries}
			cli := newClient1414(t, a)
			got, err := cli.Get(context.Background(), "x", Options{Namespace: "ns",
				GVR: schema.GroupVersionResource{Group: "g.example.org", Version: "v1", Resource: c.ask}})
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			want := "/apis/g.example.org/v1/namespaces/ns/" + c.want + "/x"
			if paths := a.objectPaths(); len(paths) != 1 || paths[0] != want || got.GetLabels()["collection"] != c.want {
				t.Fatalf("asked %q: dialled %v, want exactly [%s]", c.ask, paths, want)
			}
		})
	}
}

func TestDiscoveryDocument_NamesAreMatchedWithoutRegardToCase(t *testing.T) {
	for _, ask := range []string{"Foos", "foos", "FOOS", "Foo", "foo"} {
		t.Run(ask, func(t *testing.T) {
			a := &oddDocumentApiserver{entries: docEntry("Foos", "Foo")}
			cli := newClient1414(t, a)
			_, err := cli.Get(context.Background(), "x", Options{Namespace: "ns",
				GVR: schema.GroupVersionResource{Group: "g.example.org", Version: "v1", Resource: ask}})
			if IsNotServed(err) {
				t.Fatalf("asked %q: a served type was called not served", ask)
			}
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			// The path carries the document's own name.
			const want = "/apis/g.example.org/v1/namespaces/ns/Foos/x"
			if paths := a.objectPaths(); len(paths) != 1 || paths[0] != want {
				t.Fatalf("asked %q: dialled %v, want exactly [%s]", ask, paths, want)
			}
		})
	}
	// And a name the document does not list in any case is still not served.
	a := &oddDocumentApiserver{entries: docEntry("Foos", "Foo")}
	_, err := newClient1414(t, a).Get(context.Background(), "x", Options{Namespace: "ns",
		GVR: schema.GroupVersionResource{Group: "g.example.org", Version: "v1", Resource: "bars"}})
	if !IsNotServed(err) || len(a.objectPaths()) != 0 {
		t.Fatalf("an unlisted name must be not served with nothing dialled, got %v; paths %v", err, a.objectPaths())
	}
}
