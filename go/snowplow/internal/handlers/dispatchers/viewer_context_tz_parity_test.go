package dispatchers

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// With tz declared, the four key consumers fold it identically, through the one
// effectiveKeyExtras: dispatch lookup, widgetContent lookup, subscription
// arming (both classes) and the boot/keepwarm seed (no request extras, so the
// no-tz cell). An invalid tz on a subscription coordinate is "not sent" there
// too, so it arms the same key a dispatch of that request Puts under.
func TestViewerContextTZ_KeyParity_DispatchWidgetContentSubscriptionSeed(t *testing.T) {
	buildF6SharedBindingWatcher(t, []string{"tz"})
	ctx := f6CtxUser("userA")
	const (
		g, v, r, ns, name = "widgets.templates.krateo.io", "v1beta1", "panels", "demo", "panel-1"
	)
	cr := h1WidgetUnstructured(map[string]any{"keyExtras": []any{"tz"}})
	pp, pg := normalizePagination(0, 0)

	dispatchKey := func(extras map[string]any) string {
		k, h, _ := dispatchCacheLookupKey(ctx, "widgets", g, v, r, ns, name, pp, pg, effectiveKeyExtras(ctx, cr.Object, extras))
		if h == nil || k == "" {
			t.Fatal("no dispatch key")
		}
		return k
	}
	contentKey := func(extras map[string]any) string {
		k, h, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, pp, pg, effectiveKeyExtras(ctx, cr.Object, extras))
		if h == nil || k == "" {
			t.Fatal("no widgetContent key")
		}
		return k
	}
	sub := func(class string, extras map[string]any) string {
		k, ok := DeriveSubscriptionKey(ctx, SubscriptionCoordinates{
			Class: class, Group: g, Version: v, Resource: r, Namespace: ns, Name: name, Extras: extras})
		if !ok || k == "" {
			t.Fatalf("subscription (%s) did not arm", class)
		}
		return k
	}
	rome := map[string]any{"tz": "Europe/Rome"}
	utc := map[string]any{"tz": "UTC"}
	bad := map[string]any{"tz": "UTC'); DROP"}

	for _, c := range []struct {
		class string
		key   func(map[string]any) string
	}{{classWidgets, dispatchKey}, {cache.CacheEntryClassWidgetContent, contentKey}} {
		t.Run(c.class, func(t *testing.T) {
			if c.key(rome) == c.key(utc) || c.key(rome) == c.key(nil) {
				t.Fatal("a declared tz must partition the key (Rome, UTC, no-tz are three cells)")
			}
			if got, want := sub(c.class, rome), c.key(rome); got != want {
				t.Fatalf("subscription key for tz=Rome %q != dispatch key %q", got, want)
			}
			if got, want := sub(c.class, utc), c.key(utc); got != want {
				t.Fatalf("subscription key for tz=UTC %q != dispatch key %q", got, want)
			}
			// the seed has no request extras: it IS the no-tz cell.
			if got, want := sub(c.class, nil), c.key(nil); got != want {
				t.Fatalf("seed/no-tz subscription key %q != no-tz key %q", got, want)
			}
			if got, want := sub(c.class, bad), c.key(nil); got != want {
				t.Fatalf("an invalid tz on a subscription must arm the no-tz key; got %q want %q", got, want)
			}
		})
	}
}
