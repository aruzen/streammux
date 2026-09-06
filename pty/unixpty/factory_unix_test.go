//go:build darwin || linux

package unixpty_test

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/aruzen/streammux/pty"
	"github.com/aruzen/streammux/pty/unixpty"
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
