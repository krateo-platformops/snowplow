package api

// cluster_list_traceid_once_916_test.go — #916: the cluster-list async populate
// carries its request's logger into a context of its own. A line it writes
// carries traceId once, and it is the request's.
//
// Without the site's fix the populate's context has no traceId, so
// xcontext.WithLogger stamps an empty one after the request's: the line holds
// two copies (before the decorator's fix), or only the empty one (after it).

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	templatesapi "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/testsupport/jsonkeys"
	"github.com/krateo-platformops/snowplow/internal/tracing"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type buf916 struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *buf916) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *buf916) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestS916_ClusterListPopulateLineCarriesTheRequestsTraceIdOnce(t *testing.T) {
	ResetClusterListAsyncStateForTest()
	t.Cleanup(func() {
		WaitClusterListAsyncIdleForTest()
		ResetClusterListAsyncStateForTest()
	})

	const msg = "s916.populate.warn"
	done := make(chan struct{})
	prev := populateClusterListCellSyncFn
	populateClusterListCellSyncFn = func(ctx context.Context, _ *slog.Logger, _ *templatesapi.API, _ schema.GroupVersionResource, _ string, _ httpcall.RequestOptions, _ *cache.ResolvedCacheStore) bool {
		// The populate path logs through the context's logger.
		xcontext.Logger(ctx).WarnContext(ctx, msg)
		close(done)
		return true
	}
	t.Cleanup(func() { populateClusterListCellSyncFn = prev })

	sink := &buf916{}
	root := slog.New(tracing.NewTraceCorrelationHandler(slog.NewJSONHandler(sink, nil)))
	// The request: its traceId and its request logger, as RequestLogger builds them.
	customerCtx := xcontext.BuildContext(context.Background(),
		xcontext.WithTraceId("s916Req"),
		xcontext.WithLogger(root),
	)

	populateClusterListCellAsync(customerCtx, xcontext.Logger(customerCtx), &templatesapi.API{Name: "s916"},
		endpoints.Endpoint{}, f1WidgetsGVR, "s916-cell", httpcall.RequestOptions{}, cache.ResolvedCache())

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("PRECONDITION: the populate goroutine did not run within 5s")
	}

	var line string
	for _, l := range strings.Split(strings.TrimSpace(sink.String()), "\n") {
		if strings.Contains(l, msg) {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("PRECONDITION: the populate's line never reached the sink: %s", sink.String())
	}
	got, err := jsonkeys.Read([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	if got.Count("traceId") != 1 || len(got.Repeated) > 0 {
		t.Errorf("#916 RED: the populate's line carries traceId %d times (repeats %v): %s", got.Count("traceId"), got.Repeated, line)
	}
	if !strings.Contains(line, `"traceId":"s916Req"`) {
		t.Errorf("#916 RED: the populate's line lost its request's traceId: %s", line)
	}
}
