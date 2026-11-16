//go:build !(js && wasm)

package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A non-positive --jwks-request-timeout must not mean "no timeout": an
// unbounded JWKS fetch can hang every authentication. snowplow refuses it at
// startup with errJWKSRequestTimeoutNotPositive.

func TestCheckJWKSRequestTimeout_RefusesANonPositiveTimeout(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Nanosecond, -5 * time.Second} {
		err := checkJWKSRequestTimeout(d)
		if !errors.Is(err, errJWKSRequestTimeoutNotPositive) {
			t.Errorf("checkJWKSRequestTimeout(%s) = %v, want errJWKSRequestTimeoutNotPositive", d, err)
		}
	}
	for _, d := range []time.Duration{time.Nanosecond, 5 * time.Second} {
		if err := checkJWKSRequestTimeout(d); err != nil {
			t.Errorf("checkJWKSRequestTimeout(%s) = %v, want nil", d, err)
		}
	}
}

// The real binary, started as an operator starts it: a non-positive timeout,
// by flag or by env, exits 1 with the named error before anything is served.
// The controls (the default, and an explicit positive value) get past the
// check to the next startup refusal, an empty JWKS URL, so the refusal is the
// timeout's and not something earlier failing.
func TestStartup_RefusesANonPositiveJWKSRequestTimeout(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go command on PATH")
	}
	bin := filepath.Join(t.TempDir(), "snowplow")
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	run := func(t *testing.T, env []string, args ...string) (int, string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, bin, args...)
		cmd.Dir = t.TempDir()
		cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + cmd.Dir}, env...)
		out, err := cmd.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatalf("snowplow %v was still running after a minute; it did not refuse at startup\n%s", args, out)
		}
		var ee *exec.ExitError
		if err != nil && !errors.As(err, &ee) {
			t.Fatalf("snowplow %v: %v", args, err)
		}
		return cmd.ProcessState.ExitCode(), string(out)
	}
	const named = "--jwks-request-timeout/JWT_JWKS_REQUEST_TIMEOUT must be positive"

	for _, tc := range []struct {
		name string
		env  []string
		args []string
		got  string
	}{
		{"flag 0", nil, []string{"--jwks-request-timeout=0s"}, "(got 0s)"},
		{"flag negative", nil, []string{"--jwks-request-timeout=-1s"}, "(got -1s)"},
		{"env 0", []string{"JWT_JWKS_REQUEST_TIMEOUT=0s"}, nil, "(got 0s)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out := run(t, tc.env, append(tc.args, "--jwks-url=http://127.0.0.1:1/.well-known/jwks.json")...)
			if code != 1 || !strings.Contains(out, named) || !strings.Contains(out, tc.got) {
				t.Fatalf("exit %d, want 1 with %q and %q; output:\n%s", code, named, tc.got, out)
			}
		})
	}
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"default (5s)", nil},
		{"flag 5s", []string{"--jwks-request-timeout=5s"}},
	} {
		t.Run("control "+tc.name, func(t *testing.T) {
			code, out := run(t, nil, append(tc.args, "--jwks-url=", "--url-authn=")...)
			if strings.Contains(out, named) {
				t.Fatalf("a positive timeout was refused:\n%s", out)
			}
			if code != 1 || !strings.Contains(out, "no JWKS URL") {
				t.Fatalf("exit %d, want 1 at the next startup check (no JWKS URL), which proves the timeout check passed; output:\n%s", code, out)
			}
		})
	}
}
