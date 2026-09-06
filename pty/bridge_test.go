package pty_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/aruzen/streammux"
	"github.com/aruzen/streammux/pty"
)

type memoryProcess struct {
	output io.Reader
	input  bytes.Buffer
}

func TestBridgeAndCustomProtocolShareConnection(t *testing.T) {
	left, right := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := streammux.Open(ctx, left, streammux.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := streammux.Open(ctx, right, streammux.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	process := &memoryProcess{output: bytes.NewReader([]byte("pty-output"))}
	bridge, err := pty.NewBridge(process, server, pty.BridgeConfig{StreamID: 5, InputType: 10, OutputType: 11})
	if err != nil {
		t.Fatal(err)
	}
	bridgeDone := make(chan error, 1)
	go func() { bridgeDone <- bridge.RunOutput(ctx) }()
	control, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 12, Flags: streammux.FlagEvent, StreamID: 5}, []byte("custom-event"))
	controlDone := make(chan error, 1)
	go func() { controlDone <- server.Send(ctx, control) }()
	seen := make(map[streammux.MessageType]string)
	for range 2 {
		frame, readErr := client.ReadFrame()
		if readErr != nil {
			t.Fatal(readErr)
		}
		seen[frame.Header.MessageType] = string(frame.Payload)
	}
	if seen[11] != "pty-output" || seen[12] != "custom-event" {
		t.Fatalf("frames = %#v", seen)
	}
	if err = <-bridgeDone; err != nil {
		t.Fatal(err)
	}
	if err = <-controlDone; err != nil {
		t.Fatal(err)
	}
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
