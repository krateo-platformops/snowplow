//go:build scale765 || unit || integration

// viewed_test.go: the #765 VIEWED arm. Diego's ruling: no time-based refresh
// logic; refreshes stay on the refresher + SSE path, with HIGHER priority for a
// cell somebody is viewing. This arm makes "somebody is viewing" real: a few
// users hold the dashboard page open through the production /refreshes route
// (one SSE subscription on the page's 5 dashboard-data widgets, each push
// answered by a re-GET of that widget, as the SPA does), and it measures the
// SUBSCRIBED cells against an equal number of UNSUBSCRIBED cells of other
// identity classes:
//
//   - dirty share: the fraction of 500 ms samples in which the cell's inputs
//     had moved since its last accepted Put (cache.InputsMovedSincePut, a
//     read-only probe of the dep tracker's stamp);
//   - closed windows: the harness's own clean->dirty->clean intervals per cell
//     (sample resolution 500 ms), i.e. dirty_to_fresh per group;
//   - body age: seconds since the cell's last accepted Put, as served
//     (ResolvedCacheStore.MetadataForKey AgeSeconds);
//   - pushes per subscribed cell and the viewer's re-fetch latency.
//
// Nothing here is computed by the code under test: the refresher's own
// dirty_to_fresh histogram has no key, so it cannot split the groups.
package scale765_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

type s765ViewedCell struct {
	user   s765User
	widget s765Widget
	key    string
	class  string
}

type s765CellTrack struct {
	dirtySamples, samples, absent int
	openSince                     time.Time
	windowsMS                     []float64
	ages                          []float64
	pushes                        int
}

type s765Viewed struct {
	h          *s765H
	subscribed []s765ViewedCell
	control    []s765ViewedCell

	mu        sync.Mutex
	track     map[string]*s765CellTrack // by key
	refetchMS []float64
	connected atomic.Int32
	streamErr atomic.Int32
}

// s765ViewedGroup is one group's figures.
type s765ViewedGroup struct {
	Cells          int      `json:"cells"`
	DirtyShare     float64  `json:"dirty_share"`
	AbsentShare    float64  `json:"absent_share"` // samples where the cell was not resident (evicted)
	ClosedWindows  s765Dist `json:"closed_dirty_windows_ms"`
	OpenAtEnd      int      `json:"windows_open_at_end"`
	BodyAgeSeconds s765Dist `json:"served_body_age_s"`
	PushesPerCell  float64  `json:"sse_pushes_per_cell"`
}

// s765ViewedReport is the arm's result.
type s765ViewedReport struct {
	Viewers          int             `json:"viewers"`
	StreamsConnected int             `json:"sse_streams_connected"`
	StreamErrors     int             `json:"sse_stream_errors"`
	Subscribed       s765ViewedGroup `json:"subscribed"`
	Unsubscribed     s765ViewedGroup `json:"unsubscribed"`
	ViewerRefetchMS  s765Dist        `json:"viewer_refetch_on_push_ms"`
}

// startViewed picks 2·n identity classes (n viewers, n controls), reads each
// one's dashboard-data widget cell keys from the real /call response headers,
// opens the viewers' SSE streams and starts the 500 ms sampler. Everything
// stops with ctx; the samples count only while measuring is set.
func (h *s765H) startViewed(ctx context.Context, wg *sync.WaitGroup, n int, measuring *atomic.Bool) *s765Viewed {
	v := &s765Viewed{h: h, track: map[string]*s765CellTrack{}}
	seen := map[string]bool{}
	var picks []s765User
	for _, u := range h.users {
		g := append([]string(nil), u.groups...)
		sort.Strings(g)
		k := strings.Join(g, ",")
		if seen[k] || u.name == h.probeUser().name {
			continue
		}
		seen[k] = true
		picks = append(picks, u)
		if len(picks) == 2*n {
			break
		}
	}
	if len(picks) < 2*n {
		h.t.Fatalf("viewed arm: %d identity classes, need %d", len(picks), 2*n)
	}
	for i, u := range picks {
		for _, w := range h.widgets {
			if w.apiRef != "dashboard-data" {
				continue
			}
			req, _ := http.NewRequest(http.MethodGet, s765WidgetPath(w), nil)
			req.Header.Set("Authorization", "Bearer "+u.token)
			rec := &s765HeaderRecorder{h: http.Header{}}
			h.call.ServeHTTP(rec, req)
			key, class := rec.h.Get("X-Snowplow-Refresh-Key"), rec.h.Get("X-Snowplow-Refresh-Class")
			if rec.code != http.StatusOK || key == "" {
				h.t.Fatalf("viewed arm: %s %s: HTTP %d, refresh key %q", u.name, w.name, rec.code, key)
			}
			c := s765ViewedCell{user: u, widget: w, key: key, class: class}
			if i < n {
				v.subscribed = append(v.subscribed, c)
			} else {
				v.control = append(v.control, c)
			}
			v.track[key] = &s765CellTrack{}
		}
	}
	// The viewers' streams: one POST /refreshes per viewer, its page's cells.
	byUser := map[string][]s765ViewedCell{}
	for _, c := range v.subscribed {
		byUser[c.user.name] = append(byUser[c.user.name], c)
	}
	for _, cells := range byUser {
		cells := cells
		wg.Add(1)
		go func() { defer wg.Done(); v.stream(ctx, cells, measuring) }()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if measuring.Load() {
				v.sample()
			}
		}
	}()
	return v
}

// stream holds one viewer's subscription and answers each push with a re-GET.
func (v *s765Viewed) stream(ctx context.Context, cells []s765ViewedCell, measuring *atomic.Bool) {
	type sub struct {
		Class     string `json:"class"`
		Group     string `json:"group"`
		Version   string `json:"version"`
		Resource  string `json:"resource"`
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
		PerPage   int    `json:"perPage"`
		Page      int    `json:"page"`
	}
	var subs []sub
	byKey := map[string]s765ViewedCell{}
	for _, c := range cells {
		subs = append(subs, sub{Class: c.class, Group: "widgets.templates.krateo.io", Version: "v1beta1",
			Resource: c.widget.plural, Namespace: s765RelNS, Name: c.widget.name, PerPage: -1, Page: -1})
		byKey[c.key] = c
	}
	body, _ := json.Marshal(subs)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, v.h.sse.URL+"/refreshes", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+cells[0].user.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			v.streamErr.Add(1)
		}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		v.streamErr.Add(1)
		return
	}
	v.connected.Add(1)
	sc := bufio.NewScanner(resp.Body)
	event := ""
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: ") && event == "refresh":
			key := strings.TrimPrefix(line, "data: ")
			c, ok := byKey[key]
			if !ok || !measuring.Load() {
				continue
			}
			_, _, d := v.h.get(c.user, s765WidgetPath(c.widget)) // the SPA's re-fetch
			v.mu.Lock()
			v.track[key].pushes++
			v.refetchMS = append(v.refetchMS, float64(d.Microseconds())/1000)
			v.mu.Unlock()
		case line == "":
			event = ""
		}
	}
}

// sample reads every tracked cell once.
func (v *s765Viewed) sample() {
	store := cache.ResolvedCache()
	now := time.Now()
	v.mu.Lock()
	defer v.mu.Unlock()
	for key, tr := range v.track {
		tr.samples++
		meta, ok := store.MetadataForKey(key)
		if !ok {
			tr.absent++
			tr.openSince = time.Time{} // an evicted cell's window ends with it
			continue
		}
		tr.ages = append(tr.ages, float64(meta.AgeSeconds))
		if cache.InputsMovedSincePut(key) {
			tr.dirtySamples++
			if tr.openSince.IsZero() {
				tr.openSince = now
			}
		} else if !tr.openSince.IsZero() {
			tr.windowsMS = append(tr.windowsMS, float64(now.Sub(tr.openSince).Milliseconds()))
			tr.openSince = time.Time{}
		}
	}
}

func (v *s765Viewed) group(cells []s765ViewedCell) s765ViewedGroup {
	g := s765ViewedGroup{Cells: len(cells)}
	var dirty, samples, absent, pushes int
	var windows, ages []float64
	for _, c := range cells {
		tr := v.track[c.key]
		dirty += tr.dirtySamples
		samples += tr.samples
		absent += tr.absent
		pushes += tr.pushes
		windows = append(windows, tr.windowsMS...)
		ages = append(ages, tr.ages...)
		if !tr.openSince.IsZero() {
			g.OpenAtEnd++
		}
	}
	if samples > absent {
		g.DirtyShare = float64(dirty) / float64(samples-absent)
	}
	if samples > 0 {
		g.AbsentShare = float64(absent) / float64(samples)
	}
	g.ClosedWindows = s765Summarize(windows)
	g.BodyAgeSeconds = s765Summarize(ages)
	if len(cells) > 0 {
		g.PushesPerCell = float64(pushes) / float64(len(cells))
	}
	return g
}

func (v *s765Viewed) report() *s765ViewedReport {
	v.mu.Lock()
	defer v.mu.Unlock()
	return &s765ViewedReport{
		Viewers:          len(v.subscribed) / 5,
		StreamsConnected: int(v.connected.Load()),
		StreamErrors:     int(v.streamErr.Load()),
		Subscribed:       v.group(v.subscribed),
		Unsubscribed:     v.group(v.control),
		ViewerRefetchMS:  s765Summarize(v.refetchMS),
	}
}

// s765HeaderRecorder is a minimal ResponseWriter that keeps the headers and
// the status (the body is discarded).
type s765HeaderRecorder struct {
	h    http.Header
	code int
}

func (r *s765HeaderRecorder) Header() http.Header { return r.h }
func (r *s765HeaderRecorder) WriteHeader(c int) {
	if r.code == 0 {
		r.code = c
	}
}
func (r *s765HeaderRecorder) Write(b []byte) (int, error) {
	if r.code == 0 {
		r.code = http.StatusOK
	}
	return len(b), nil
}
