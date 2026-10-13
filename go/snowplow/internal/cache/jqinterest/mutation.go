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
	mutBuiltinVarsFirst    = "builtin-vars-first"    // $ENV is answered as the built-in before a user binding of $ENV
	mutPigConstWins        = "pig-const-wins"        // a stage named extras/slice is shadowed by the constant (handler.go does the opposite)
	mutNoExtrasSeed        = "no-extras-seed"        // dict[stage] is never pre-seeded by an extras key with the stage's name
	mutKeyFlatAND          = "key-flat-and"          // guards from separate selects are read as ONE conjunction
	mutKeyNotRead          = "key-not-read"          // the key field of a select guard is not recorded in the read set (R2)
	mutHasAsKind           = "has-as-kind"           // has(k) observes only the kind of its input (R5)
	mutLiteralValuesSkip   = "literal-values-skip"   // a key constant's non-literal alternative is SKIPPED instead of making Values unknown (N4)
	mutRRTNotStrict        = "rrt-not-strict"        // resourcesRefs fields not analysed strictly
	mutOtherStagesSkip     = "other-stages-skipped"  // other stages' templates reading the list are ignored
	mutDelExclParent       = "del-excl-parent"       // del(.a.b) excludes .a (too wide)
	mutKeyNoTotality       = "key-no-totality"       // membership ignores errors on non-members
	mutStrictNoReceiver    = "strict-no-receiver"    // strict analysis does not observe a navigation's receiver
	mutKeyNoAggregation    = "key-no-aggregation"    // membership ignores aggregation over non-members
	mutUnionDecidable      = "union-decidable"       // a union key constant gets its first extras alternative's source (#633)
	mutAltAsNav            = "alt-as-nav"            // `<extras chain> // <literal>` is read as the chain alone: the default is dropped (#633)
)
