// Fixture: the sources of truth, intact (R0 must pass on them).
package cache

type ResolvedKeyInputs struct {
	CacheEntryClass   string
	Name              string
	BindingUID        string
	SubjectBindingSet string
	RBACSubGen        uint64
}

type IdentityClass struct {
	SubjectBindingSet string
	RBACSubGen        uint64
}

const (
	CacheEntryClassWidgetContent = "widgetContent"
	CacheEntryClassApistage      = "apistage"
)

var identityFreeClasses = map[string]int{
	CacheEntryClassWidgetContent: 1,
	CacheEntryClassApistage:      2,
}

func IsIdentityFreeClass(class string) bool { _, ok := identityFreeClasses[class]; return ok }

func (in *ResolvedKeyInputs) SetIdentity(bindingUID string, class IdentityClass) {
	in.BindingUID = bindingUID
	in.SubjectBindingSet = class.SubjectBindingSet
	in.RBACSubGen = class.RBACSubGen
}

func ComputeKey(in ResolvedKeyInputs) string {
	s := in.CacheEntryClass + in.Name
	if !IsIdentityFreeClass(in.CacheEntryClass) {
		s += in.BindingUID + in.SubjectBindingSet + string(rune(in.RBACSubGen))
	}
	return s
}

func RBACSubGenForSubject(username string, groups []string) uint64 { return 0 }

type SeedResolveMemo struct{ m map[string]any }

func (m *SeedResolveMemo) Key(ns, name, user string, groups []string, class string) string {
	return ns + name + user + class
}

func SeedResolveMemoFromContext(ctx any) *SeedResolveMemo { return nil }

// GOOD builder: takes the class and writes it through SetIdentity.
func BoundKeyInputs(name, bindingUID string, class IdentityClass) ResolvedKeyInputs {
	in := ResolvedKeyInputs{CacheEntryClass: "raFullList", Name: name}
	in.SetIdentity(bindingUID, class)
	return in
}

// GOOD test seam (exempt from R3; no production caller).
func BoundKeyInputsForTest(name, bindingUID string) ResolvedKeyInputs {
	return BoundKeyInputs(name, bindingUID, IdentityClass{})
}

// BAD (R5): an unregistered content store.
var widgetBodyMemo = map[string][]byte{}
