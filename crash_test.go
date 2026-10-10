// SPDX-License-Identifier: MIT

package kv

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// crashChildEnv names the directory the child process writes to; the child
// test is skipped when it is unset.
const crashChildEnv = "ERMITE_KV_CRASH_CHILD"

// crashIterations returns ERMITE_KV_CRASH_ITERATIONS (default 20).
func crashIterations(t *testing.T) int {
	v := os.Getenv("ERMITE_KV_CRASH_ITERATIONS")
	if v == "" {
		return 20
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		t.Fatalf("ERMITE_KV_CRASH_ITERATIONS=%q: want a positive integer", v)
	}
	return n
}

// crashOptions makes the child compact every few dozen batches.
func crashOptions() Options { return Options{CompactAfter: 20 * recSize} }

// TestCrashChild is the writer run by TestCrash in a child process. It
// continues the sequence k<i> → v<i> where the previous run stopped and prints
// the seq of each acknowledged batch on its own line, until it is killed.
func TestCrashChild(t *testing.T) {
	dir := os.Getenv(crashChildEnv)
	if dir == "" {
		t.Skip("only runs as the child of TestCrash")
	}
	db, err := Open(bg, dir, crashOptions())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	deadline := time.Now().Add(time.Minute) // do not outlive a parent that died
	for i := int(db.Snapshot(bg).Seq()) + 1; time.Now().Before(deadline); i++ {
		if err := applySeq(db, i); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
		// One write per line: the parent never sees half of a number.
		if _, err := os.Stdout.WriteString(strconv.Itoa(i) + "\n"); err != nil {
			t.Fatalf("report %d: %v", i, err)
		}
	}
}

// ackReader records the highest seq the child reported and keeps every
// other line (test failures are written to stdout).
type ackReader struct {
	mu    sync.Mutex
	rest  []byte
	last  int
	other strings.Builder
}

func (a *ackReader) Write(p []byte) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rest = append(a.rest, p...)
	for {
		line, rest, ok := bytes.Cut(a.rest, []byte{'\n'})
		if !ok {
			return len(p), nil
		}
		if n, err := strconv.Atoi(string(line)); err == nil {
			a.last = max(a.last, n)
		} else {
			a.other.Write(line)
			a.other.WriteByte('\n')
		}
		a.rest = rest
	}
}

func (a *ackReader) output() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.other.String()
}

func (a *ackReader) lastAcked() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.last
}

// TestCrash checks AC2 against SIGKILL: a child process applies sequential
// batches with NoSync=false and frequent compactions on a real directory and
// is killed at a random time. After each kill the database must reopen, hold
// every batch the child reported as acknowledged, and be exactly the batches
// 1..N for some N at least as large. The next child continues from there, so
// the directory goes through every kind of interrupted compaction.
func TestCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	seed := powerLossBase(t)
	t.Logf("seed %d (ERMITE_KV_SEED=%d reproduces the kill delays)", seed, seed)
	rng := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	total := 0
	for i := range crashIterations(t) {
		acks := new(ackReader)
		cmd := exec.Command(exe, "-test.run=^TestCrashChild$", "-test.count=1")
		cmd.Env = append(os.Environ(), crashChildEnv+"="+dir)
		cmd.Stdout = acks
		var stderr strings.Builder
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		delay := time.Duration(20+rng.IntN(180)) * time.Millisecond
		time.Sleep(delay)
		_ = cmd.Process.Kill()
		// Wait also waits for the stdout copy. The child must have died from
		// the kill: an earlier exit means the writer itself failed.
		werr := cmd.Wait()
		var exitErr *exec.ExitError
		if !errors.As(werr, &exitErr) || exitErr.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			t.Fatalf("iteration %d: child did not die from SIGKILL (%v)\nchild output: %s\nchild stderr: %s",
				i, werr, acks.output(), stderr.String())
		}
		acked := acks.lastAcked()

		db, err := Open(bg, dir, Options{})
		if err != nil {
			t.Fatalf("iteration %d (killed after %v, %d acknowledged, seed %d): reopen: %v\nchild stderr: %s",
				i, delay, acked, seed, err, stderr.String())
		}
		n, err := checkSequential(db.Snapshot(bg), acked)
		if cerr := db.Close(bg); err == nil {
			err = cerr
		}
		if err != nil {
			t.Fatalf("iteration %d (killed after %v, %d acknowledged, seed %d): %v", i, delay, acked, seed, err)
		}
		total = n
	}
	if total == 0 {
		t.Fatal("the child never acknowledged a batch")
	}
	t.Logf("%d batches recovered after %d kills", total, crashIterations(t))
}
