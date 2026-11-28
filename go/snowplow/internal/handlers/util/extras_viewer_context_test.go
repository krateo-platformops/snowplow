package util_test

import (
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/handlers/util"
)

// A tz that is not an IANA zone name is dropped at the request edge, on BOTH
// extras channels (query and POST body), so everything downstream sees the
// request as if it had not been sent. A real zone and every other key pass
// through untouched.
func TestParseExtras_DropsInvalidViewerContextTZ_BothChannels(t *testing.T) {
	bad := []string{"UTC'); DROP", "Local", "", "Europe/Rome\n", "Mars/Base", "'"}
	for _, tz := range bad {
		t.Run(tz, func(t *testing.T) {
			extras := map[string]any{"tz": tz, "name": "n"}
			want := map[string]any{"name": "n"}

			// body channel (POST /call/read)
			r, _ := http.NewRequest("GET", "/call", nil)
			r = r.WithContext(util.WithExtras(r.Context(), extras))
			got, err := util.ParseExtras(r)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("body channel: got %#v err %v, want %#v", got, err, want)
			}

			// query channel
			q := url.Values{"extras": {`{"name":"n","tz":` + jsonString(tz) + `}`}}
			r2, _ := http.NewRequest("GET", "/call?"+q.Encode(), nil)
			got, err = util.ParseExtras(r2)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("query channel: got %#v err %v, want %#v", got, err, want)
			}
		})
	}
}

func TestParseExtras_KeepsValidViewerContextTZ_BothChannels(t *testing.T) {
	want := map[string]any{"tz": "Europe/Rome", "name": "n"}
	r, _ := http.NewRequest("GET", "/call", nil)
	r = r.WithContext(util.WithExtras(r.Context(), map[string]any{"tz": "Europe/Rome", "name": "n"}))
	if got, _ := util.ParseExtras(r); !reflect.DeepEqual(got, want) {
		t.Fatalf("body channel dropped a valid tz: %#v", got)
	}
	q := url.Values{"extras": {`{"name":"n","tz":"Europe/Rome"}`}}
	r2, _ := http.NewRequest("GET", "/call?"+q.Encode(), nil)
	if got, _ := util.ParseExtras(r2); !reflect.DeepEqual(got, want) {
		t.Fatalf("query channel dropped a valid tz: %#v", got)
	}
}

func jsonString(s string) string {
	b := []byte{'"'}
	for _, c := range []byte(s) {
		switch c {
		case '"', '\\':
			b = append(b, '\\', c)
		case '\n':
			b = append(b, '\\', 'n')
		default:
			b = append(b, c)
		}
	}
	return string(append(b, '"'))
}
