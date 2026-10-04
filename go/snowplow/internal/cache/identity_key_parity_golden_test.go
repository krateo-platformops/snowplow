// identity_key_parity_golden_test.go — #449 key-parity golden.
//
// #449 routes every identity-dimension write through ResolvedKeyInputs.SetIdentity
// and makes ComputeKey read the identity-free class set instead of a literal
// widgetContent test. Neither change may move a key: a moved key is a cold L1 on
// the rolling restart, and resolvedKeyVersion was NOT bumped for #449.
//
// The hex values below were captured by running this table against origin/main
// c7533929 (pre-#449 ComputeKey) and are asserted byte-for-byte here. Every class
// is covered, with every input ComputeKey folds set to a non-default value on at
// least one row (identity, pagination, stage, extras), and with the inputs each
// class's real mint site produces (apistage and widgetContent mint with zero
// identity).
package cache

import "testing"

func identityKeyParityRows() []struct {
	name string
	in   ResolvedKeyInputs
	want string
} {
	bound := func(class string) ResolvedKeyInputs {
		return ResolvedKeyInputs{
			CacheEntryClass: class,
			Group:           "templates.krateo.io", Version: "v1", Resource: "restactions",
			Namespace: "krateo-system", Name: "golden-ra",
			BindingUID:        "C:uid-golden",
			SubjectBindingSet: "5f1c0a7e2b9d4c3a8e6f0d1b2c3a4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d",
			RBACSubGen:        7,
			PerPage:           10,
			Page:              2,
			Extras:            map[string]any{"tenant": "acme", "n": float64(3)},
		}
	}
	return []struct {
		name string
		in   ResolvedKeyInputs
		want string
	}{
		{"restactions/full", bound("restactions"), "cdd0bb5cf0bd963d2b99d1856e2820f3f589c8fc009cb27e5a004064343cc0e1"},
		{"restactions/unpaginated-no-extras", ResolvedKeyInputs{CacheEntryClass: "restactions", Group: "templates.krateo.io", Version: "v1", Resource: "restactions", Namespace: "krateo-system", Name: "golden-ra", BindingUID: "R:krateo-system/uid-rb", SubjectBindingSet: "aa", RBACSubGen: 0, PerPage: -1, Page: -1}, "34c088e169a20b366268990a2e85bc233d71cd3da95ab3029961d15ec0bfd8df"},
		{"widgets/full", func() ResolvedKeyInputs {
			in := bound("widgets")
			in.Group, in.Version, in.Resource = "widgets.templates.krateo.io", "v1beta1", "panels"
			return in
		}(), "d84f6254aef47f38be549592cc2380974bf6c9129fe55061019bd65c8457de3c"},
		{"raFullList/full", func() ResolvedKeyInputs {
			in := bound(CacheEntryClassRAFullList)
			in.PerPage, in.Page = 0, 0
			return in
		}(), "3de832056cea9ef08701323348635f933dce7f6b08da9bb8e243813899f253ed"},
		{"widgetContent/mint-shape", ResolvedKeyInputs{CacheEntryClass: CacheEntryClassWidgetContent, Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels", Namespace: "krateo-system", Name: "golden-panel", PerPage: 5, Page: 1, Extras: map[string]any{"route": "x"}}, "1eca98be1ec9fb8960abd6000eded644d610c142901745ed88f22e3c003af3d0"},
		{"widgetContent/identity-set-is-ignored", func() ResolvedKeyInputs {
			in := bound(CacheEntryClassWidgetContent)
			return in
		}(), "4aacddac1d11d2cdd2f44e37ffb86806780b7c496b86801be755f2dcacd3aa86"},
		{"apistage/list-mint-shape", ResolvedKeyInputs{CacheEntryClass: CacheEntryClassApistage, Version: "v1", Resource: "configmaps", Namespace: "ns-a"}, "ecda53b1c68b911b971c4c7c218561133226570e215e437f9b679f1462ee2c8b"},
		{"apistage/get-mint-shape", ResolvedKeyInputs{CacheEntryClass: CacheEntryClassApistage, Group: "composition.krateo.io", Version: "v1-2-0", Resource: "fireworksapps", Namespace: "demo", Name: "app-1"}, "3337c75d6384b4d7130668fb01ce43b5530fd5129d4daa26b7bda6812a0a9d36"},
		{"apistage/cluster-list-mint-shape", ResolvedKeyInputs{CacheEntryClass: CacheEntryClassApistage, Group: "composition.krateo.io", Version: "v1-2-0", Resource: "fireworksapps"}, "08501649345fb421710595b475f32773e7ffb1c4e10dc877d790ddb29f4d848d"},
		{"stage-folded", func() ResolvedKeyInputs {
			in := bound("restactions")
			in.Stage = "stage-1|filterhash|inputhash"
			return in
		}(), "d53b6edc545594fb97ac1023b4b4aa8bf1f82d184f374135eb40e0ceefe4da10"},
	}
}

func TestIdentityKeyParityGolden_ComputeKey(t *testing.T) {
	for _, r := range identityKeyParityRows() {
		got := ComputeKey(r.in)
		if got != r.want {
			t.Errorf("%s: ComputeKey moved: got %s want %s", r.name, got, r.want)
		}
	}
}
