package streammux_test

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/aruzen/streammux"
)

func TestConnSerializesConcurrentFrames(t *testing.T) {
	clientStream, serverStream := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := streammux.Open(ctx, clientStream, streammux.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := streammux.Open(ctx, serverStream, streammux.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	const count = 32
	read := make(chan map[streammux.CorrelationID]bool, 1)
	go func() {
		seen := make(map[streammux.CorrelationID]bool)
		for range count {
			frame, readErr := server.ReadFrame()
			if readErr != nil {
				read <- nil
				return
			}
			seen[frame.Header.CorrelationID] = true
		}
		read <- seen
	}()
	var wait sync.WaitGroup
	for index := range count {
		wait.Add(1)
		go func() {
			defer wait.Done()
			id := streammux.CorrelationID(index + 1)
			frame, frameErr := streammux.NewOwnedFrame(streammux.Header{Version: 1, MessageType: 4, Flags: streammux.FlagRequest, CorrelationID: id}, []byte{byte(index)})
			if frameErr != nil || client.SendOwned(ctx, frame) != nil {
				t.Errorf("send %d: %v", index, frameErr)
			}
		}()
	}
	wait.Wait()
	seen := <-read
	if len(seen) != count {
		t.Fatalf("received = %d", len(seen))
	}
}

func TestContextCancellationUnblocksRead(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	ctx, cancel := context.WithCancel(context.Background())
	conn, err := streammux.Open(ctx, left, streammux.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, readErr := conn.ReadFrame(); result <- readErr }()
	cancel()
	if err = <-result; err == nil {
		t.Fatal("ReadFrame succeeded after cancellation")
	}
	if closeErr := conn.Close(); closeErr != nil {
		t.Fatalf("Close = %v", closeErr)
	}
}
