package jqinterest

// testMutations is the RED-first seam for the soundness arms (#603 PR-1). Each
// name switches OFF one rule of the analysis (an over-narrowing), and the
// differential fuzz must then find a counterexample — an S-arm that no
// mutation can turn RED is not a detector. nil in production: every check is a
// map lookup on a nil map, which is always false.
var testMutations map[string]bool

func mutated(name string) bool { return testMutations[name] }

// Mutation names (see mutation_arms_test.go for the arm each one turns RED).
const (
	mutNavNoKind           = "nav-no-kind"           // navigation does not observe the kind along the path
	mutIterNoShape         = "iter-no-shape"         // `.[]` does not observe the length / key set
	mutLengthAsKind        = "length-as-kind"        // length/keys observe only the kind
	mutEqAsKind            = "eq-as-kind"            // ==/!= observe only the kind of the operands
	mutAltNoTruth          = "alt-no-truth"          // `//` does not observe the truthiness of its left side
	mutSelectNoTruth       = "select-no-truth"       // select does not observe its predicate
	mutIfNoTruth           = "if-no-truth"           // if does not observe its condition
	mutSortByAsNav         = "sortby-as-nav"         // sort_by(f) treated as navigation: f's value not observed
	mutGenericNoInput      = "generic-no-input"      // a generic builtin does not observe its input
	mutCatchNotStrict      = "catch-not-strict"      // try/catch does not upgrade to whole reads
	mutUAFExcluded         = "uaf-excluded"          // the UAF namespaceFrom/nameFrom reads are dropped
	mutUAFNameFromExcluded = "uaf-namefrom-excluded" // only the UAF nameFrom read is dropped (name-specific verbs)
	mutRRTNotStrict        = "rrt-not-strict"        // resourcesRefs fields not analysed strictly
	mutOtherStagesSkip     = "other-stages-skipped"  // other stages' templates reading the list are ignored
	mutDelExclParent       = "del-excl-parent"       // del(.a.b) excludes .a (too wide)
	mutKeyNoTotality       = "key-no-totality"       // membership ignores errors on non-members
	mutStrictNoReceiver    = "strict-no-receiver"    // strict analysis does not observe a navigation's receiver
	mutKeyNoAggregation    = "key-no-aggregation"    // membership ignores aggregation over non-members
)
