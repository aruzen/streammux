package pty_test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	"github.com/aruru-weed/streammux"
	"github.com/aruru-weed/streammux/pty"
)

type memoryProcess struct {
	output io.Reader
	input  bytes.Buffer
}

func (p *memoryProcess) Output() io.Reader     { return p.output }
func (p *memoryProcess) Input() io.Writer      { return &p.input }
func (p *memoryProcess) Resize(pty.Size) error { return nil }
func (p *memoryProcess) Wait() error           { return nil }
func (p *memoryProcess) Kill() error           { return nil }
func (p *memoryProcess) Close() error          { return nil }

type collectingSender struct {
	mu     sync.Mutex
	frames []streammux.Frame
}

func (s *collectingSender) SendOwned(_ context.Context, frame streammux.Frame) error {
	s.mu.Lock()
	s.frames = append(s.frames, frame)
	s.mu.Unlock()
	return nil
}

func TestBridgeSeparatesPTYDataFromCustomControl(t *testing.T) {
	process := &memoryProcess{output: bytes.NewReader([]byte{0, 0xff, 0x1b, 'x'})}
	sender := &collectingSender{}
	bridge, err := pty.NewBridge(process, sender, pty.BridgeConfig{Version: 1, StreamID: 7, InputType: 10, OutputType: 11})
	if err != nil {
		t.Fatal(err)
	}
	if err = bridge.RunOutput(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sender.frames) != 1 || sender.frames[0].Header.MessageType != 11 || !bytes.Equal(sender.frames[0].Payload, []byte{0, 0xff, 0x1b, 'x'}) {
		t.Fatalf("frames = %#v", sender.frames)
	}
	input, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 10, Flags: streammux.FlagEvent, StreamID: 7}, []byte("input"))
	if err = bridge.HandleInput(input); err != nil {
		t.Fatal(err)
	}
	if process.input.String() != "input" {
		t.Fatalf("input = %q", process.input.String())
	}
	control, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 12, Flags: streammux.FlagRequest, CorrelationID: 1, StreamID: 7}, nil)
	if err = bridge.HandleInput(control); err == nil {
		t.Fatal("custom control was accepted as PTY input")
	}
}
