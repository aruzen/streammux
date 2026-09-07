//go:build windows

package windowspty_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aruzen/streammux/pty"
	"github.com/aruzen/streammux/pty/windowspty"
	"golang.org/x/sys/windows"
)

func TestWindowsPTYHelperProcess(t *testing.T) {
	separator := -1
	for index, argument := range os.Args {
		if argument == "--" {
			separator = index
			break
		}
	}
	if separator < 0 || separator+1 >= len(os.Args) {
		return
	}
	mode := os.Args[separator+1]
	switch mode {
	case "interactive":
		line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		fmt.Printf("reply:%s\r\n", strings.TrimRight(line, "\r\n"))
	case "exit7":
		os.Exit(7)
	case "raw":
		_, _ = os.Stdout.Write([]byte("\x1b[31m日本語\x1b[0m"))
	case "environment":
		fmt.Printf("environment:%s\r\n", os.Getenv("STREAMMUX_ENV_TEST"))
	case "interrupt":
		fmt.Print("interrupt-ready\r\n")
		time.Sleep(time.Minute)
	case "size":
		printConsoleSize()
	case "resize":
		printConsoleSize()
		_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
		time.Sleep(100 * time.Millisecond)
		printConsoleSize()
	case "parent":
		executable, _ := os.Executable()
		child := exec.Command(executable, "-test.run=^TestWindowsPTYHelperProcess$", "--", "wait")
		child.Stdin, child.Stdout, child.Stderr = os.Stdin, os.Stdout, os.Stderr
		if err := child.Start(); err != nil {
			os.Exit(91)
		}
		fmt.Printf("child-pid:%d\r\n", child.Process.Pid)
		_ = child.Wait()
	case "orphan-parent":
		executable, _ := os.Executable()
		child := exec.Command(executable, "-test.run=^TestWindowsPTYHelperProcess$", "--", "wait")
		child.Stdin, child.Stdout, child.Stderr = os.Stdin, os.Stdout, os.Stderr
		if err := child.Start(); err != nil {
			os.Exit(93)
		}
		fmt.Printf("child-pid:%d\r\n", child.Process.Pid)
	case "wait":
		time.Sleep(time.Minute)
	default:
		os.Exit(92)
	}
	os.Exit(0)
}

func printConsoleSize() {
	var information windows.ConsoleScreenBufferInfo
	if err := windows.GetConsoleScreenBufferInfo(windows.Handle(os.Stdout.Fd()), &information); err != nil {
		fmt.Printf("size-error:%v\r\n", err)
		return
	}
	cols := information.Window.Right - information.Window.Left + 1
	rows := information.Window.Bottom - information.Window.Top + 1
	rows++ // ConPTY reserves its final row for the cursor.
	if rows > information.Size.Y {
		rows = information.Size.Y
	}
	fmt.Printf("size:%d:%d\r\n", cols, rows)
}

func helperSpec(t *testing.T, mode string, size pty.Size) pty.ProcessSpec {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return pty.ProcessSpec{
		Command:     executable,
		Args:        []string{"-test.run=^TestWindowsPTYHelperProcess$", "--", mode},
		InitialSize: size,
	}
}

func TestManagedInteractiveIOAndTypedExit(t *testing.T) {
	process, err := (windowspty.ManagedFactory{}).StartManaged(context.Background(), helperSpec(t, "interactive", pty.Size{Cols: 80, Rows: 24}))
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	if _, err = io.WriteString(process.Input(), "hello\r\n"); err != nil {
		t.Fatal(err)
	}
	output, status := collect(t, process)
	if !bytes.Contains(output, []byte("reply:hello")) {
		t.Fatalf("output = %q", output)
	}
	if status.Reason != pty.ExitReasonExited || status.Code != 0 {
		t.Fatalf("ExitStatus = %#v", status)
	}
}

func TestManagedCmdOutput(t *testing.T) {
	process, err := (windowspty.ManagedFactory{}).StartManaged(context.Background(), pty.ProcessSpec{
		Command: "cmd.exe", Args: []string{"/d", "/c", "echo cmd-ok"}, InitialSize: pty.Size{Cols: 80, Rows: 24},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	output, status := collect(t, process)
	if !bytes.Contains(output, []byte("cmd-ok")) || status.Code != 0 {
		t.Fatalf("output = %q, ExitStatus = %#v", output, status)
	}
}

func TestLegacyFactoryInteractiveIO(t *testing.T) {
	spec := helperSpec(t, "interactive", pty.Size{})
	process, err := (windowspty.Factory{}).Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	if _, err = io.WriteString(process.Input(), "legacy\r\n"); err != nil {
		t.Fatal(err)
	}
	outputDone := make(chan []byte, 1)
	go func() {
		output, _ := io.ReadAll(process.Output())
		outputDone <- output
	}()
	waitDone := make(chan error, 1)
	go func() { waitDone <- process.Wait() }()
	select {
	case output := <-outputDone:
		if !bytes.Contains(output, []byte("reply:legacy")) {
			t.Fatalf("output = %q", output)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("legacy ConPTY output timed out")
	}
	select {
	case err = <-waitDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("legacy ConPTY wait timed out")
	}
}

func TestManagedInitialSizeAndResize(t *testing.T) {
	process, err := (windowspty.ManagedFactory{}).StartManaged(context.Background(), helperSpec(t, "resize", pty.Size{Cols: 101, Rows: 33}))
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	reader := bufio.NewReader(process.Output())
	initial := readUntil(t, reader, "size:101:33")
	if !strings.Contains(initial, "size:101:33") {
		t.Fatalf("initial size output = %q", initial)
	}
	if err = process.Resize(pty.Size{Cols: 120, Rows: 40}); err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(process.Input(), "continue\r\n"); err != nil {
		t.Fatal(err)
	}
	resized := readUntil(t, reader, "size:120:40")
	if !strings.Contains(resized, "size:120:40") {
		t.Fatalf("resized output = %q", resized)
	}
	if _, err = io.ReadAll(reader); err != nil {
		t.Fatal(err)
	}
	status, err := process.WaitStatus()
	if err != nil || status.Code != 0 {
		t.Fatalf("WaitStatus = %#v, %v", status, err)
	}
}

func TestManagedPreservesUTF8AndVT(t *testing.T) {
	process, err := (windowspty.ManagedFactory{}).StartManaged(context.Background(), helperSpec(t, "raw", pty.Size{Cols: 80, Rows: 24}))
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	output, status := collect(t, process)
	for _, want := range [][]byte{[]byte("\x1b[31m"), []byte("日本語"), []byte("\x1b[m")} {
		if !bytes.Contains(output, want) {
			t.Fatalf("ConPTY output = %x, want it to contain %x", output, want)
		}
	}
	if status.Code != 0 {
		t.Fatalf("ExitStatus = %#v", status)
	}
}

func TestManagedExplicitEnvironment(t *testing.T) {
	spec := helperSpec(t, "environment", pty.Size{Cols: 80, Rows: 24})
	spec.Env = append(os.Environ(), "STREAMMUX_ENV_TEST=works")
	process, err := (windowspty.ManagedFactory{}).StartManaged(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	output, status := collect(t, process)
	if !bytes.Contains(output, []byte("environment:works")) || status.Code != 0 {
		t.Fatalf("output = %q, ExitStatus = %#v", output, status)
	}
}

func TestManagedTerminateSendsConsoleClose(t *testing.T) {
	process, err := (windowspty.ManagedFactory{}).StartManaged(context.Background(), helperSpec(t, "interrupt", pty.Size{Cols: 80, Rows: 24}))
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	reader := bufio.NewReader(process.Output())
	ready := readUntil(t, reader, "interrupt-ready")
	if !strings.Contains(ready, "interrupt-ready") {
		t.Fatalf("ready output = %q", ready)
	}
	if err = process.Terminate(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct {
		status pty.ExitStatus
		err    error
	}, 1)
	go func() {
		status, waitErr := process.WaitStatus()
		done <- struct {
			status pty.ExitStatus
			err    error
		}{status, waitErr}
	}()
	select {
	case result := <-done:
		if result.err != nil || result.status.Reason != pty.ExitReasonSignaled {
			t.Fatalf("WaitStatus = %#v, %v", result.status, result.err)
		}
	case <-time.After(10 * time.Second):
		_ = process.Kill()
		t.Fatal("Terminate did not stop ConPTY client")
	}
	_, _ = io.ReadAll(reader)
}

func TestManagedNonzeroExit(t *testing.T) {
	process, err := (windowspty.ManagedFactory{}).StartManaged(context.Background(), helperSpec(t, "exit7", pty.Size{Cols: 80, Rows: 24}))
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	_, status := collect(t, process)
	if status.Reason != pty.ExitReasonExited || status.Code != 7 {
		t.Fatalf("ExitStatus = %#v", status)
	}
}

func TestManagedKillTerminatesJobProcessTree(t *testing.T) {
	process, err := (windowspty.ManagedFactory{}).StartManaged(context.Background(), helperSpec(t, "parent", pty.Size{Cols: 80, Rows: 24}))
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	reader := bufio.NewReader(process.Output())
	line := readUntil(t, reader, "child-pid:")
	match := regexp.MustCompile(`child-pid:(\d+)`).FindStringSubmatch(line)
	if len(match) != 2 {
		t.Fatalf("child PID output = %q", line)
	}
	pid, _ := strconv.ParseUint(match[1], 10, 32)
	child, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(child)
	if err = process.Kill(); err != nil {
		t.Fatal(err)
	}
	status, err := process.WaitStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.Reason != pty.ExitReasonKilled {
		t.Fatalf("ExitStatus = %#v", status)
	}
	if result, err := windows.WaitForSingleObject(child, 5000); err != nil || result != windows.WAIT_OBJECT_0 {
		t.Fatalf("grandchild survived Job Object termination: result=%#x err=%v", result, err)
	}
	_, _ = io.ReadAll(reader)
}

func TestManagedKillTerminatesTreeAfterRootExit(t *testing.T) {
	process, err := (windowspty.ManagedFactory{}).StartManaged(context.Background(), helperSpec(t, "orphan-parent", pty.Size{Cols: 80, Rows: 24}))
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	reader := bufio.NewReader(process.Output())
	line := readUntil(t, reader, "child-pid:")
	match := regexp.MustCompile(`child-pid:(\d+)`).FindStringSubmatch(line)
	if len(match) != 2 {
		t.Fatalf("child PID output = %q", line)
	}
	pid, _ := strconv.ParseUint(match[1], 10, 32)
	child, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(child)
	status, err := process.WaitStatus()
	if err != nil || status.Reason != pty.ExitReasonExited || status.Code != 0 {
		t.Fatalf("root WaitStatus = %#v, %v", status, err)
	}
	if err = process.Kill(); err != nil {
		t.Fatal(err)
	}
	if result, err := windows.WaitForSingleObject(child, 5000); err != nil || result != windows.WAIT_OBJECT_0 {
		t.Fatalf("grandchild survived post-root Job Object termination: result=%#x err=%v", result, err)
	}
	_, _ = io.ReadAll(reader)
}

func TestManagedKillWaitCloseRace(t *testing.T) {
	for range 25 {
		process, err := (windowspty.ManagedFactory{}).StartManaged(context.Background(), helperSpec(t, "wait", pty.Size{Cols: 80, Rows: 24}))
		if err != nil {
			t.Fatal(err)
		}
		var wait sync.WaitGroup
		wait.Add(3)
		go func() { defer wait.Done(); _ = process.Kill() }()
		go func() { defer wait.Done(); _, _ = process.WaitStatus() }()
		go func() { defer wait.Done(); _ = process.Close() }()
		wait.Wait()
		status, err := process.WaitStatus()
		if err != nil {
			t.Fatal(err)
		}
		if status.Reason != pty.ExitReasonKilled {
			t.Fatalf("ExitStatus = %#v", status)
		}
		if err = process.Kill(); err != nil {
			t.Fatalf("Kill after WaitStatus = %v", err)
		}
	}
}

func TestManagerDrainsProcessWithoutAttachment(t *testing.T) {
	manager, err := pty.NewManager(context.Background(), windowspty.ManagedFactory{}, pty.DefaultManagerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	session, err := manager.Open(context.Background(), helperSpec(t, "raw", pty.Size{Cols: 80, Rows: 24}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := session.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Reason != pty.ExitReasonExited || status.Code != 0 {
		t.Fatalf("ExitStatus = %#v", status)
	}
}

func collect(t *testing.T, process pty.ManagedProcess) ([]byte, pty.ExitStatus) {
	t.Helper()
	outputDone := make(chan struct {
		data []byte
		err  error
	}, 1)
	go func() {
		data, err := io.ReadAll(process.Output())
		outputDone <- struct {
			data []byte
			err  error
		}{data, err}
	}()
	waitDone := make(chan struct {
		status pty.ExitStatus
		err    error
	}, 1)
	go func() {
		status, err := process.WaitStatus()
		waitDone <- struct {
			status pty.ExitStatus
			err    error
		}{status, err}
	}()
	var output []byte
	var status pty.ExitStatus
	deadline := time.After(10 * time.Second)
	for outputDone != nil || waitDone != nil {
		select {
		case result := <-outputDone:
			if result.err != nil {
				t.Fatal(result.err)
			}
			output = result.data
			outputDone = nil
		case result := <-waitDone:
			if result.err != nil {
				t.Fatal(result.err)
			}
			status = result.status
			waitDone = nil
		case <-deadline:
			_ = process.Kill()
			t.Fatal("ConPTY collection timed out")
		}
	}
	return output, status
}

func readUntil(t *testing.T, reader *bufio.Reader, marker string) string {
	t.Helper()
	type result struct {
		text string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		var collected strings.Builder
		for {
			line, err := reader.ReadString('\n')
			collected.WriteString(line)
			if strings.Contains(collected.String(), marker) || err != nil {
				done <- result{text: collected.String(), err: err}
				return
			}
		}
	}()
	select {
	case value := <-done:
		if value.err != nil && !errorsIsEOF(value.err) {
			t.Fatal(value.err)
		}
		return value.text
	case <-time.After(10 * time.Second):
		t.Fatal("ConPTY output timed out")
		return ""
	}
}

func errorsIsEOF(err error) bool { return err == io.EOF }
