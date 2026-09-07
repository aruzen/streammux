//go:build darwin || linux

package unixpty_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aruzen/streammux/pty"
	"github.com/aruzen/streammux/pty/unixpty"
	"golang.org/x/sys/unix"
)

func TestInteractiveProcessIOAndExit(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("Unix PTY test")
	}
	process, err := (unixpty.Factory{}).Start(context.Background(), pty.ProcessSpec{Command: "/bin/sh", Args: []string{"-c", "read line; printf 'reply:%s\\n' \"$line\""}})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	if err = process.Resize(pty.Size{Cols: 120, Rows: 40}); err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(process.Input(), "hello\n"); err != nil {
		t.Fatal(err)
	}
	result := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(process.Output())
		result <- data
	}()
	select {
	case output := <-result:
		if !bytes.Contains(output, []byte("reply:hello")) {
			t.Fatalf("output = %q", output)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PTY output timed out")
	}
	if err = process.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestRepeatedCreateClose(t *testing.T) {
	for range 10 {
		process, err := (unixpty.Factory{}).Start(context.Background(), pty.ProcessSpec{Command: "/bin/sh"})
		if err != nil {
			t.Fatal(err)
		}
		if err = process.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProcessExitCode(t *testing.T) {
	process, err := (unixpty.Factory{}).Start(context.Background(), pty.ProcessSpec{
		Command: "/bin/sh", Args: []string{"-c", "exit 7"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	if err = process.Wait(); err == nil {
		t.Fatal("Wait succeeded for non-zero exit")
	}
	exitCoder, ok := process.(interface{ ExitCode() int })
	if !ok {
		t.Fatal("process does not expose ExitCode")
	}
	if code := exitCoder.ExitCode(); code != 7 {
		t.Fatalf("ExitCode = %d, want 7", code)
	}
}

func TestManagedProcessInitialSizeAndTypedExit(t *testing.T) {
	process, err := (unixpty.ManagedFactory{}).StartManaged(context.Background(), pty.ProcessSpec{
		Command: "/bin/sh", Args: []string{"-c", "stty size; exit 7"}, InitialSize: pty.Size{Cols: 101, Rows: 33},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	output, err := io.ReadAll(process.Output())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(output, []byte("33 101")) {
		t.Fatalf("initial stty size output = %q", output)
	}
	status, err := process.WaitStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.Reason != pty.ExitReasonExited || status.Code != 7 {
		t.Fatalf("ExitStatus = %#v", status)
	}
}

func TestManagedStartContextDoesNotOwnRunningProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	process, err := (unixpty.ManagedFactory{}).StartManaged(ctx, pty.ProcessSpec{Command: "/bin/sh", Args: []string{"-c", "sleep 30"}, InitialSize: pty.Size{Cols: 80, Rows: 24}})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	waited := make(chan pty.ExitStatus, 1)
	go func() { status, _ := process.WaitStatus(); waited <- status }()
	select {
	case status := <-waited:
		t.Fatalf("Start context stopped process: %#v", status)
	case <-time.After(50 * time.Millisecond):
	}
	if err = process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case status := <-waited:
		if status.Reason != pty.ExitReasonKilled {
			t.Fatalf("killed status = %#v", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("managed process was not reaped")
	}
	if err = process.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestManagedProcessPreservesBinaryUTF8AndVTBytes(t *testing.T) {
	process, err := (unixpty.ManagedFactory{}).StartManaged(context.Background(), pty.ProcessSpec{
		Command: "/bin/sh", Args: []string{"-c", "printf '\\001\\033[31m日本語\\033[0m\\377'"}, InitialSize: pty.Size{Cols: 80, Rows: 24},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	output, err := io.ReadAll(process.Output())
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("\x01\x1b[31m日本語\x1b[0m\xff")
	if !bytes.Contains(output, want) {
		t.Fatalf("raw PTY output = %x, want it to contain %x", output, want)
	}
	if _, err = process.WaitStatus(); err != nil {
		t.Fatal(err)
	}
}

func TestManagedTerminateSignalsGrandchildProcessGroup(t *testing.T) {
	process, err := (unixpty.ManagedFactory{}).StartManaged(context.Background(), pty.ProcessSpec{
		Command: "/bin/sh", Args: []string{"-c", "sleep 30 & child=$!; echo $child; wait $child"}, InitialSize: pty.Size{Cols: 80, Rows: 24},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	line, err := bufio.NewReader(process.Output()).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		t.Fatalf("child pid output = %q", line)
	}
	childPID, err := strconv.Atoi(fields[0])
	if err != nil {
		t.Fatalf("child pid output = %q: %v", line, err)
	}
	if err = process.Terminate(); err != nil {
		t.Fatal(err)
	}
	if _, err = process.WaitStatus(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		err = unix.Kill(childPID, 0)
		if errors.Is(err, unix.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("grandchild pid %d survived process-group termination", childPID)
}

func TestManagedKillWaitCloseRace(t *testing.T) {
	for range 50 {
		process, err := (unixpty.ManagedFactory{}).StartManaged(context.Background(), pty.ProcessSpec{
			Command: "/bin/sh", Args: []string{"-c", "exit 0"}, InitialSize: pty.Size{Cols: 80, Rows: 24},
		})
		if err != nil {
			t.Fatal(err)
		}
		var wait sync.WaitGroup
		wait.Add(3)
		go func() { defer wait.Done(); _ = process.Kill() }()
		go func() { defer wait.Done(); _, _ = process.WaitStatus() }()
		go func() { defer wait.Done(); _ = process.Close() }()
		wait.Wait()
		if _, err = process.WaitStatus(); err != nil {
			t.Fatal(err)
		}
		if err = process.Kill(); err != nil {
			t.Fatalf("Kill after WaitStatus = %v", err)
		}
	}
}
