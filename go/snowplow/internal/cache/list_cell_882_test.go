package cache

import "testing"

// TestListCell882_JSONValueLenStepsOverExactlyOneItem — the anchor steps over an
// envelope item whose bytes are not the listing's with jsonValueLen (#882 F2).
// Braces, brackets, quotes and escapes inside strings must not end the item
// early, and anything that is not an object or array is refused.
func TestListCell882_JSONValueLenStepsOverExactlyOneItem(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int
		ok   bool
	}{
		{`{"a":1},{"b":2}`, len(`{"a":1}`), true},
		{`{"s":"}{][,"},{}`, len(`{"s":"}{][,"}`), true},
		{`{"s":"a\"}b\\"},{}`, len(`{"s":"a\"}b\\"}`), true},
		{`{"n":{"m":[1,{"x":"]"}]}}]}`, len(`{"n":{"m":[1,{"x":"]"}]}}`), true},
		{`[1,2],[3]`, len(`[1,2]`), true},
		{`{"unterminated":1`, 0, false},
		{`"a string"`, 0, false},
		{` {"leading space":1}`, 0, false},
		{`1,{}`, 0, false},
		{``, 0, false},
	} {
		got, ok := jsonValueLen([]byte(tc.in))
		if got != tc.want || ok != tc.ok {
			t.Errorf("jsonValueLen(%q) = (%d, %v), want (%d, %v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}
