package api

// slice_cursor_874_test.go — #874: a paginated resolve hands the data source
// its own continuation token as .slice.cursor, next to page/perPage/offset; a
// resolve without a cursor builds exactly the pre-#874 slice.

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"testing"

	"github.com/krateo-platformops/plumbing/jwtutil"
)

func TestS874_PaginatedDictCarriesTheCursor(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cases := []struct {
		name string
		opts ResolveOptions
		want any
	}{
		{"cursor", ResolveOptions{PerPage: 5, Page: 2, Cursor: "c5"},
			map[string]any{"page": 2, "perPage": 5, "offset": 5, "cursor": "c5"}},
		{"no cursor (pre-#874 shape)", ResolveOptions{PerPage: 5, Page: 2},
			map[string]any{"page": 2, "perPage": 5, "offset": 5}},
		{"unpaginated: no slice, cursor ignored", ResolveOptions{Cursor: "c5"}, nil},
	}
	for _, c := range cases {
		r := newResolveRun(context.Background(), c.opts, log, jwtutil.UserInfo{})
		got, ok := r.dict["slice"]
		if c.want == nil {
			if ok {
				t.Fatalf("%s: an unpaginated resolve must not build a slice, got %v", c.name, got)
			}
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Fatalf("%s: #874 RED: dict.slice = %v, want %v", c.name, got, c.want)
		}
	}
}
