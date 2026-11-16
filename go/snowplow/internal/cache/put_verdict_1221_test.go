package cache

// put_verdict_1221_test.go — #1221: the external-TTL opt-in qualifies only when every
// touch carried nothing of the requester. These pin the rule on the sinks, through a
// child sink and through a memo replay. Real dispatch is in
// handlers/dispatchers/issue1221_external_ttl_third_party_test.go.

import (
	"context"
	"testing"
)

func TestIssue1221_TheOptInNeedsEveryTouchToBeThirdParty(t *testing.T) {
	cases := []struct {
		name        string
		plain       int
		thirdParty  int
		forbExt     int64
		stageErrors int64
		want        bool
	}{
		{name: "one third-party touch", thirdParty: 1, want: true},
		{name: "many third-party touches", thirdParty: 3, want: true},
		{name: "a plain touch alone", plain: 1},
		{name: "a plain touch beside a third-party one", plain: 1, thirdParty: 1},
		{name: "a plain touch beside many third-party ones", plain: 1, thirdParty: 9},
		// The requester's refused dial is taken off the external count, but it
		// was still that requester's dial: the body holds their answer.
		{name: "an own-credential 403 beside a third-party touch", plain: 1, thirdParty: 1, forbExt: 1},
		{name: "a stage error beside a third-party touch", thirdParty: 1, stageErrors: 1},
		{name: "no touch at all"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := freshSinks(nil, nil, nil)
			for i := 0; i < c.plain; i++ {
				g.External.Bump()
			}
			for i := 0; i < c.thirdParty; i++ {
				g.External.BumpThirdParty()
			}
			g.Forbidden.external.Store(c.forbExt)
			g.Stage.count.Store(c.stageErrors)
			v := PutGateVerdict(g, "widgets").WithExternalTTL(true)
			if got := v.ExternalTTL(); got != c.want {
				t.Fatalf("ExternalTTL()=%v, want %v: %+v", got, c.want, v)
			}
		})
	}
}

func TestIssue1221_OnlyThirdPartyIsNilSafeAndFalseOnNoTouch(t *testing.T) {
	var nilSink *ExternalTouchedSink
	nilSink.BumpThirdParty()
	if nilSink.OnlyThirdParty() {
		t.Fatal("a nil sink must not qualify")
	}
	if (&ExternalTouchedSink{}).OnlyThirdParty() {
		t.Fatal("no touch must not qualify")
	}
}

func TestIssue1221_ChildSinksForwardTheThirdPartyCount(t *testing.T) {
	ctx, parent := WithExternalTouchedSink(context.Background())
	_, child := WithChildPutGateSinks(ctx)
	child.External.BumpThirdParty()
	if !parent.OnlyThirdParty() || !child.External.OnlyThirdParty() {
		t.Fatalf("a child's third-party touch must qualify both: parent=%v child=%v", parent.OnlyThirdParty(), child.External.OnlyThirdParty())
	}
	child.External.Bump()
	if parent.OnlyThirdParty() {
		t.Fatal("a child's plain touch must disqualify the parent too")
	}
}

// A memo hit is judged as the resolve it replaces, in both directions.
func TestIssue1221_AMemoReplayKeepsTheThirdPartyCount(t *testing.T) {
	replayed := func(record func(*ExternalTouchedSink)) *ExternalTouchedSink {
		ctx, c := WithPutGateEffectsCapture(context.Background())
		record(ExternalTouchedSinkFromContext(ctx))
		into, sink := WithExternalTouchedSink(context.Background())
		c.Effects().Replay(into)
		return sink
	}
	if s := replayed(func(s *ExternalTouchedSink) { s.BumpThirdParty() }); !s.OnlyThirdParty() {
		t.Fatalf("a replayed third-party touch must still qualify: count=%d", s.Count())
	}
	if s := replayed(func(s *ExternalTouchedSink) { s.BumpThirdParty(); s.Bump() }); s.OnlyThirdParty() || s.Count() != 2 {
		t.Fatalf("a replayed mixed resolve must not qualify: count=%d", s.Count())
	}
}
