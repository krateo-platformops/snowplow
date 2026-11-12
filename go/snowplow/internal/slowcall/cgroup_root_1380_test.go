package slowcall

// cgroup_root_1380_test.go — #1380: cpu.stat is read beneath cgroupDir opened as
// an os.Root. Without the Root (no cgroup filesystem) and without a cpu.stat in
// it, a sample carries no cgroup fields, as before; a cpu.stat that resolves
// outside the Root is not read. The Root is kept once opened, and a directory
// that does not exist is settled as nil; any other open failure is retried at
// the next sample.

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// countingOpen1380 is os.OpenRoot behind fail: the first len(fail) calls
// return those errors, later ones open for real. calls counts every call.
func countingOpen1380(fail ...error) (open func(string) (*os.Root, error), calls *int) {
	n := 0
	return func(dir string) (*os.Root, error) {
		n++
		if n <= len(fail) {
			return nil, &fs.PathError{Op: "openat", Path: dir, Err: fail[n-1]}
		}
		return os.OpenRoot(dir)
	}, &n
}

// A transient failure (EMFILE) at the first sample is not kept: the next
// sample opens the Root and carries cgroup fields, and the samples after it
// reuse that Root without opening again.
func TestCgroupRoot1380_TransientOpenFailureIsRetried(t *testing.T) {
	dir := t.TempDir()
	write1380(t, filepath.Join(dir, "cpu.stat"), v2Stat1380)
	open, calls := countingOpen1380(syscall.EMFILE)
	c := &rootCache{dir: dir, open: open}
	t.Cleanup(func() {
		if c.root != nil {
			_ = c.root.Close()
		}
	})

	if s := readCPUFrom(c.get()); s.cgOK {
		t.Fatalf("sample 1, open failed with EMFILE, yet cgOK: %+v", s)
	}
	if s := readCPUFrom(c.get()); !s.cgOK || s.periods != 40 {
		t.Fatalf("sample 2, after a transient open failure: want the cgroup fields, got %+v", s)
	}
	for range 3 {
		if s := readCPUFrom(c.get()); !s.cgOK {
			t.Fatalf("a later sample lost the cgroup fields: %+v", s)
		}
	}
	if *calls != 2 {
		t.Fatalf("open called %d times over 5 samples, want 2 (the failure, then one success kept)", *calls)
	}
}

// A directory that does not exist is settled as nil: no cgroup fields, and no
// open at later samples.
func TestCgroupRoot1380_MissingDirectoryIsSettledAsNil(t *testing.T) {
	open, calls := countingOpen1380(fs.ErrNotExist, fs.ErrNotExist)
	c := &rootCache{dir: t.TempDir(), open: open}
	for i := range 3 {
		if r := c.get(); r != nil {
			t.Fatalf("get %d returned a Root for a directory that does not exist", i+1)
		}
	}
	if *calls != 1 {
		t.Fatalf("open called %d times for a directory that does not exist, want 1", *calls)
	}

	// The real os.OpenRoot on a missing directory is classified the same way.
	missing := &rootCache{dir: filepath.Join(t.TempDir(), "no-such-cgroup-fs"), open: os.OpenRoot}
	if missing.get() != nil || !missing.settled {
		t.Fatalf("os.OpenRoot on a missing directory: root %v settled %v, want nil and settled", missing.root, missing.settled)
	}
}

const v2Stat1380 = "usage_usec 5000\nuser_usec 1\nsystem_usec 2\nnr_periods 40\nnr_throttled 7\nthrottled_usec 9000\n"

func openRoot1380(t *testing.T, dir string) *os.Root {
	t.Helper()
	r, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func write1380(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// No Root (the cgroup filesystem cannot be opened): the process CPU is still
// measured, the cgroup is not.
func TestCgroupRoot1380_NoRootCarriesNoCgroupFields(t *testing.T) {
	s := readCPUFrom(nil)
	if s.cgOK {
		t.Fatalf("readCPUFrom(nil) = cgOK with no cgroup root: %+v", s)
	}
	if _, procOK := processCPU(); s.procOK != procOK {
		t.Fatalf("readCPUFrom(nil).procOK = %v, want processCPU's %v", s.procOK, procOK)
	}
}

// A Root with no cpu.stat in any of cgroupStatPaths: no cgroup fields, the
// behaviour of a missing file.
func TestCgroupRoot1380_MissingFileCarriesNoCgroupFields(t *testing.T) {
	if s := readCPUFrom(openRoot1380(t, t.TempDir())); s.cgOK {
		t.Fatalf("no cpu.stat under the root, yet cgOK: %+v", s)
	}
}

// v2's unified cpu.stat and v1's cpu,cpuacct/cpu.stat are read beneath the root.
func TestCgroupRoot1380_ReadsV2AndV1BeneathTheRoot(t *testing.T) {
	v2 := t.TempDir()
	write1380(t, filepath.Join(v2, "cpu.stat"), v2Stat1380)
	if s := readCPUFrom(openRoot1380(t, v2)); !s.cgOK || s.usageNS != 5000*1000 || s.periods != 40 || s.thrPer != 7 || s.thrNS != 9000*1000 {
		t.Fatalf("v2 cpu.stat under the root: %+v", s)
	}

	v1 := t.TempDir()
	write1380(t, filepath.Join(v1, "cpu,cpuacct", "cpu.stat"), "nr_periods 3\nnr_throttled 2\nthrottled_time 123456\n")
	if s := readCPUFrom(openRoot1380(t, v1)); !s.cgOK || s.periods != 3 || s.thrPer != 2 || s.thrNS != 123456 || s.usageNS != -1 {
		t.Fatalf("v1 cpu,cpuacct/cpu.stat under the root: %+v", s)
	}

	// v1 where the controller is mounted as cpuacct,cpu and cpu links to it
	// (relative, inside the root): reached through cpu/cpu.stat.
	v1link := t.TempDir()
	write1380(t, filepath.Join(v1link, "cpuacct,cpu", "cpu.stat"), "nr_periods 3\nnr_throttled 2\nthrottled_time 123456\n")
	if err := os.Symlink("cpuacct,cpu", filepath.Join(v1link, "cpu")); err != nil {
		t.Fatal(err)
	}
	if s := readCPUFrom(openRoot1380(t, v1link)); !s.cgOK || s.periods != 3 {
		t.Fatalf("v1 cpu -> cpuacct,cpu symlink inside the root: %+v", s)
	}
}

// A cpu.stat that is a symlink out of the root, relative or absolute, to a
// valid cpu.stat elsewhere: not read. os.ReadFile on the same path reads it.
func TestCgroupRoot1380_ACpuStatOutsideTheRootIsNotRead(t *testing.T) {
	base := t.TempDir()
	outside := filepath.Join(base, "outside", "cpu.stat")
	write1380(t, outside, v2Stat1380)
	for name, target := range map[string]string{"relative": filepath.Join("..", "outside", "cpu.stat"), "absolute": outside} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(base, "root-"+name)
			if err := os.MkdirAll(root, 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(root, "cpu.stat")); err != nil {
				t.Fatal(err)
			}
			if _, err := os.ReadFile(filepath.Join(root, "cpu.stat")); err != nil {
				t.Fatalf("control: os.ReadFile does not follow the link either (%v), so this arm proves nothing", err)
			}
			if s := readCPUFrom(openRoot1380(t, root)); s.cgOK {
				t.Fatalf("a cpu.stat linking out of the root (%s) was read: %+v", target, s)
			}
		})
	}
}
