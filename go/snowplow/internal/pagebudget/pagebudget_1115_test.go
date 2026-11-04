package pagebudget

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestBudget1115_DeadlineIsArrivalPlusTheGoal(t *testing.T) {
	start := time.Now()
	b, ok := From(WithCustomer(context.Background(), start))
	if !ok || !b.Deadline().Equal(start.Add(10*time.Second)) {
		t.Fatalf("budget %v ok=%v, want arrival + 10 s", b, ok)
	}
	if _, ok := From(context.Background()); ok {
		t.Fatal("a plain ctx carries a budget")
	}
	if Outcome(context.Background()) != None {
		t.Fatal("a plain ctx's outcome is not none")
	}
}

// The request's outcome is the most severe any exchange reported.
func TestBudget1115_OutcomeIsTheMostSevere(t *testing.T) {
	ctx := WithCustomer(context.Background(), time.Now())
	b, _ := From(ctx)
	if Outcome(ctx) != Within {
		t.Fatalf("fresh budget reads %v, want within", Outcome(ctx))
	}
	b.Note(Retried)
	b.Note(ExhaustedConnect)
	b.Note(Within)
	if Outcome(ctx) != ExhaustedConnect {
		t.Fatalf("outcome %v, want exhausted_connect", Outcome(ctx))
	}
}

// A8 — detaching leaves the budget, keeps every other value, drops the
// cancellation.
func TestDetach1115_LeavesTheBudgetKeepsTheRest(t *testing.T) {
	type k struct{}
	parent, cancel := context.WithCancel(WithCustomer(context.WithValue(context.Background(), k{}, "v"), time.Now()))
	d := Detach(parent)
	cancel()
	if _, ok := From(d); ok {
		t.Fatal("a detached ctx still carries the customer budget")
	}
	if d.Value(k{}) != "v" {
		t.Fatal("Detach dropped another value")
	}
	if d.Err() != nil {
		t.Fatal("Detach kept the cancellation")
	}
}

// A8 — the guard: module code detaches a request's ctx only through Detach, so
// no detached unit can inherit a customer budget.
func TestDetach1115_NoBareWithoutCancel(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..")
	re := regexp.MustCompile(`context\.WithoutCancel\(`)
	var hits []string
	err := filepath.WalkDir(root, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if de.IsDir() {
			if n := de.Name(); n == "vendor" || strings.HasPrefix(n, ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		if filepath.Dir(p) == filepath.Dir(file) {
			return nil // pagebudget itself: Detach
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(b), "\n") {
			code, _, _ := strings.Cut(line, "//")
			if re.MatchString(code) {
				rel, _ := filepath.Rel(root, p)
				hits = append(hits, rel+":"+itoa(i+1))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) > 0 {
		t.Fatalf("bare context.WithoutCancel outside pagebudget (use pagebudget.Detach, #1115): %v", hits)
	}
}

func itoa(n int) string {
	var b [20]byte
	i := len(b)
	for {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
		if n == 0 {
			return string(b[i:])
		}
	}
}

func TestBudget1115_ExhaustedErrorTextAndMatching(t *testing.T) {
	e := &ExhaustedError{Result: ExhaustedConnect, Cause: "dial", Attempts: 2}
	if !errors.Is(e, ErrExhausted) || !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("ExhaustedError does not match ErrExhausted / DeadlineExceeded")
	}
	if got, want := e.Error(), "the page's 10s budget ran out while connecting (dial) after 2 attempt(s)"; got != want {
		t.Fatalf("text %q, want %q", got, want)
	}
	w := &ExhaustedError{Result: ExhaustedAnswer, Attempts: 1, MayHaveBeenApplied: true}
	if !strings.Contains(w.Error(), "may have been applied") {
		t.Fatalf("write text %q", w.Error())
	}
}
