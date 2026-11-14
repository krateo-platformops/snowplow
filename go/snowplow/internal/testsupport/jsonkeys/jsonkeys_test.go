package jsonkeys

import (
	"encoding/json"
	"reflect"
	"testing"
)

// TestReadSeesTheCopyEncodingJSONDrops: the line #916 found on 057, as a
// shape. encoding/json keeps one traceId; Read reports two.
func TestReadSeesTheCopyEncodingJSONDrops(t *testing.T) {
	line := []byte(`{"time":"t","level":"WARN","msg":"m","traceId":"a","user":{"name":"n","groups":"g"},"traceId":"a","err":"e"}`)
	var m map[string]any
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatal(err)
	}
	if len(m) != 6 {
		t.Fatalf("CONTROL: encoding/json kept %d keys; the premise is that it folds the copy", len(m))
	}
	got, err := Read(line)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"time", "level", "msg", "traceId", "user", "traceId", "err"}; !reflect.DeepEqual(got.Top, want) {
		t.Fatalf("Top = %v, want %v", got.Top, want)
	}
	if got.Count("traceId") != 2 || !reflect.DeepEqual(got.Repeated, []string{"traceId"}) {
		t.Fatalf("Count=%d Repeated=%v", got.Count("traceId"), got.Repeated)
	}
}

// TestReadNestedAndArrays: a repeat inside a nested object (or an object in an
// array) is reported by path; the same key in two different objects is not a
// repeat.
func TestReadNestedAndArrays(t *testing.T) {
	line := []byte(`{"a":1,"g":{"k":1,"k":2,"traceId":"x"},"traceId":"y","l":[{"z":1,"z":2},3,[4]],"s":"}"}`)
	got, err := Read(line)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "g", "traceId", "l", "s"}; !reflect.DeepEqual(got.Top, want) {
		t.Fatalf("Top = %v, want %v", got.Top, want)
	}
	if want := []string{"g.k", "l[].z"}; !reflect.DeepEqual(got.Repeated, want) {
		t.Fatalf("Repeated = %v, want %v", got.Repeated, want)
	}
	if got.Count("traceId") != 1 {
		t.Fatalf("Count(traceId) = %d", got.Count("traceId"))
	}
}

func TestReadRefusesWhatIsNotOneObject(t *testing.T) {
	for _, in := range []string{`[1]`, `"s"`, `{"a":1}{"b":2}`, `{"a":`} {
		if _, err := Read([]byte(in)); err == nil {
			t.Errorf("Read(%s) = nil error", in)
		}
	}
}
