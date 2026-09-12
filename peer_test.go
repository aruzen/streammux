package streammux_test

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aruzen/streammux"
)

func TestPeerConcurrentCallAndPerStreamDispatch(t *testing.T) {
	client, server, cleanup := peerPair(t)
	defer cleanup()
	if err := server.Register(40, func(ctx context.Context, peer *streammux.Peer, request streammux.Frame) error {
		response, err := streammux.NewFrame(streammux.Header{Version: 1, MessageType: request.Header.MessageType, Flags: streammux.FlagResponse, CorrelationID: request.Header.CorrelationID, StreamID: request.Header.StreamID}, request.Payload)
		if err != nil {
			return err
		}
		return peer.Respond(ctx, request, response)
	}); err != nil {
		t.Fatal(err)
	}

	const calls = 32
	var wg sync.WaitGroup
	errs := make(chan error, calls)
	for index := 0; index < calls; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			payload := []byte(strconv.Itoa(index))
			request, err := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 40, Flags: streammux.FlagRequest, CorrelationID: 1, StreamID: streammux.StreamID(index%4 + 1)}, payload)
			if err != nil {
				errs <- err
				return
			}
			response, err := client.Call(context.Background(), request)
			if err != nil {
				errs <- err
				return
			}
			if string(response.Payload) != string(payload) {
				errs <- errors.New("response payload mismatch")
			}
		}(index)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPeerPreservesEventOrderWithinStream(t *testing.T) {
	client, server, cleanup := peerPair(t)
	defer cleanup()
	got := make(chan string, 3)
	if err := server.Register(41, func(_ context.Context, _ *streammux.Peer, frame streammux.Frame) error {
		got <- string(frame.Payload)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"one", "two", "three"} {
		frame, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 41, Flags: streammux.FlagEvent, StreamID: 9}, []byte(value))
		if err := client.Emit(context.Background(), frame); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"one", "two", "three"} {
		select {
		case value := <-got:
			if value != want {
				t.Fatalf("event = %q, want %q", value, want)
			}
		case <-time.After(time.Second):
			t.Fatal("event timed out")
		}
	}
}

func TestPeerRejectsDuplicateHandlerAndOversizedQueueItem(t *testing.T) {
	client, _, cleanup := peerPair(t)
	defer cleanup()
	handler := func(context.Context, *streammux.Peer, streammux.Frame) error { return nil }
	if err := client.Register(42, handler); err != nil {
		t.Fatal(err)
	}
	if err := client.Register(42, handler); !errors.Is(err, streammux.ErrHandlerRegistered) {
		t.Fatalf("duplicate Register = %v", err)
	}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	conn, _ := streammux.Open(context.Background(), left, streammux.DefaultConfig())
	defer conn.Close()
	peer, err := streammux.NewPeer(conn, streammux.PeerConfig{OutboundQueueBytes: 64, PerStreamQueueBytes: 64, MaxActiveStreams: 1, ControlWeight: 1, InteractiveWeight: 1, BulkWeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	frame, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 42, Flags: streammux.FlagEvent, StreamID: 1}, make([]byte, 40))
	if err = peer.Emit(context.Background(), frame); !errors.Is(err, streammux.ErrPeerQueueOverflow) {
		t.Fatalf("oversized Emit = %v", err)
	}
	if stats := peer.Stats(); stats.QueueOverflows != 1 {
		t.Fatalf("queue overflows = %d", stats.QueueOverflows)
	}
	_ = peer.Close()
}

func TestPeerRegisterHandlersIsAtomic(t *testing.T) {
	client, _, cleanup := peerPair(t)
	defer cleanup()
	handler := func(context.Context, *streammux.Peer, streammux.Frame) error { return nil }
	if err := client.Register(45, handler); err != nil {
		t.Fatal(err)
	}
	if err := client.RegisterHandlers(map[streammux.MessageType]streammux.Handler{44: handler, 45: handler}); !errors.Is(err, streammux.ErrHandlerRegistered) {
		t.Fatalf("RegisterHandlers conflict = %v", err)
	}
	if err := client.Register(44, handler); err != nil {
		t.Fatalf("partial registration remained after conflict: %v", err)
	}
}

func TestPeerIgnoresLateResponseForCanceledCall(t *testing.T) {
	client, server, cleanup := peerPair(t)
	defer cleanup()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	if err := server.Register(43, func(ctx context.Context, peer *streammux.Peer, request streammux.Frame) error {
		once.Do(func() {
			close(started)
			<-release
		})
		response, err := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 43, Flags: streammux.FlagResponse, CorrelationID: request.Header.CorrelationID, StreamID: request.Header.StreamID}, request.Payload)
		if err != nil {
			return err
		}
		return peer.Respond(ctx, request, response)
	}); err != nil {
		t.Fatal(err)
	}
	request, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 43, Flags: streammux.FlagRequest, CorrelationID: 1, StreamID: 1}, []byte("first"))
	callCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	callDone := make(chan error, 1)
	go func() { _, err := client.Call(callCtx, request); callDone <- err }()
	<-started
	if err := <-callDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled Call = %v", err)
	}
	close(release)

	request2, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 43, Flags: streammux.FlagRequest, CorrelationID: 1, StreamID: 1}, []byte("second"))
	response, err := client.Call(context.Background(), request2)
	if err != nil {
		t.Fatalf("Peer rejected call after late response: %v", err)
	}
	if string(response.Payload) != "second" {
		t.Fatalf("response = %q", response.Payload)
	}
}

func TestPeerDispatchesDifferentStreamsConcurrentlyAndReportsQueues(t *testing.T) {
	client, server, cleanup := peerPair(t)
	blocked := make(chan struct{})
	started := make(chan struct{})
	other := make(chan struct{})
	if err := server.Register(46, func(_ context.Context, _ *streammux.Peer, frame streammux.Frame) error {
		if frame.Header.StreamID == 1 {
			if string(frame.Payload) == "first" {
				close(started)
				<-blocked
			}
		} else {
			close(other)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	send := func(stream streammux.StreamID, value string) {
		frame, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 46, Flags: streammux.FlagEvent, StreamID: stream}, []byte(value))
		if err := client.Emit(context.Background(), frame); err != nil {
			t.Fatal(err)
		}
	}
	send(1, "first")
	<-started
	send(1, "queued")
	send(2, "other")
	select {
	case <-other:
	case <-time.After(time.Second):
		t.Fatal("different StreamID did not run concurrently")
	}
	stats := server.Stats()
	if stats.InboundQueueFrames != 1 || len(stats.Streams) == 0 {
		t.Fatalf("peer queue stats = %#v", stats)
	}
	close(blocked)
	cleanup()
}

func TestPeerPanicAndUnknownTypeAreCountedAsProtocolErrors(t *testing.T) {
	for _, test := range []struct {
		name     string
		register bool
	}{
		{name: "panic", register: true},
		{name: "unknown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, server, cleanup := peerPair(t)
			defer cleanup()
			if test.register {
				if err := server.Register(47, func(context.Context, *streammux.Peer, streammux.Frame) error { panic("boom") }); err != nil {
					t.Fatal(err)
				}
			}
			frame, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 47, Flags: streammux.FlagEvent, StreamID: 1}, nil)
			_ = client.Emit(context.Background(), frame)
			select {
			case <-server.Done():
			case <-time.After(time.Second):
				t.Fatal("protocol failure did not close Peer")
			}
			if stats := server.Stats(); stats.ProtocolErrors != 1 {
				t.Fatalf("protocol errors = %d", stats.ProtocolErrors)
			}
		})
	}
}

func TestPeerTrafficCounters(t *testing.T) {
	client, server, cleanup := peerPair(t)
	defer cleanup()
	if err := server.Register(48, func(ctx context.Context, peer *streammux.Peer, request streammux.Frame) error {
		response, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 48, Flags: streammux.FlagResponse, CorrelationID: request.Header.CorrelationID, StreamID: request.Header.StreamID}, []byte("response"))
		return peer.Respond(ctx, request, response)
	}); err != nil {
		t.Fatal(err)
	}
	request, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 48, Flags: streammux.FlagRequest, CorrelationID: 1, StreamID: 3}, []byte("request"))
	if _, err := client.Call(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	stats := client.Stats()
	if stats.SentFrames != 1 || stats.ReceivedFrames != 1 || stats.SentBytes != streammux.HeaderSize+7 || stats.ReceivedBytes != streammux.HeaderSize+8 {
		t.Fatalf("traffic stats = %#v", stats)
	}
}

func TestPeerInboundOverflowIsCountedAndLocalized(t *testing.T) {
	serverConfig := streammux.DefaultPeerConfig()
	serverConfig.PerStreamQueueBytes = streammux.HeaderSize + 1
	serverConfig.PerStreamQueueFrames = 1
	client, server, cleanup := peerPairConfigs(t, streammux.DefaultPeerConfig(), serverConfig)
	blocked := make(chan struct{})
	started := make(chan struct{})
	if err := server.Register(49, func(context.Context, *streammux.Peer, streammux.Frame) error {
		select {
		case <-started:
		default:
			close(started)
		}
		<-blocked
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	send := func() {
		frame, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 49, Flags: streammux.FlagEvent, StreamID: 1}, []byte{'x'})
		_ = client.Emit(context.Background(), frame)
	}
	send()
	<-started
	send()
	send()
	select {
	case <-server.Done():
	case <-time.After(time.Second):
		t.Fatal("overflow did not close offending Peer")
	}
	stats := server.Stats()
	if stats.QueueOverflows != 1 || stats.ProtocolErrors != 1 {
		t.Fatalf("overflow stats = %#v", stats)
	}
	close(blocked)
	cleanup()
}

func TestPeerInboundBackpressurePreservesBoundedQueueAndConnection(t *testing.T) {
	serverConfig := streammux.DefaultPeerConfig()
	serverConfig.PerStreamQueueBytes = streammux.HeaderSize + 1
	serverConfig.PerStreamQueueFrames = 1
	serverConfig.InboundQueuePolicy = streammux.InboundQueueBackpressure
	client, server, cleanup := peerPairConfigs(t, streammux.DefaultPeerConfig(), serverConfig)
	defer cleanup()
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	started := make(chan struct{})
	handled := make(chan byte, 3)
	var calls atomic.Uint64
	if err := server.Register(49, func(_ context.Context, _ *streammux.Peer, frame streammux.Frame) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		handled <- frame.Payload[0]
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	send := func(value byte) {
		frame, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 49, Flags: streammux.FlagEvent, StreamID: 1}, []byte{value})
		if err := client.Emit(context.Background(), frame); err != nil {
			t.Fatal(err)
		}
	}
	send('a')
	<-started
	send('b')
	send('c')

	deadline := time.Now().Add(time.Second)
	for server.Stats().InboundBackpressureWaits != 1 {
		if time.Now().After(deadline) {
			t.Fatal("Peer did not apply inbound backpressure")
		}
		time.Sleep(time.Millisecond)
	}
	stats := server.Stats()
	if stats.QueueOverflows != 0 || stats.InboundQueueBytes > serverConfig.PerStreamQueueBytes || stats.InboundQueueFrames > 1 {
		t.Fatalf("backpressure stats = %#v", stats)
	}
	select {
	case <-server.Done():
		t.Fatalf("backpressure closed Peer: %v", server.Err())
	default:
	}
	unblock()
	for _, expected := range []byte{'a', 'b', 'c'} {
		select {
		case actual := <-handled:
			if actual != expected {
				t.Fatalf("handled %q, want %q", actual, expected)
			}
		case <-time.After(time.Second):
			t.Fatalf("handler did not receive %q", expected)
		}
	}
}

func TestPeerCloseUnblocksPendingCall(t *testing.T) {
	client, server, cleanup := peerPair(t)
	started := make(chan struct{})
	release := make(chan struct{})
	if err := server.Register(50, func(context.Context, *streammux.Peer, streammux.Frame) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	request, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 50, Flags: streammux.FlagRequest, CorrelationID: 1, StreamID: 1}, nil)
	result := make(chan error, 1)
	go func() { _, err := client.Call(context.Background(), request); result <- err }()
	<-started
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, streammux.ErrPeerClosed) {
			t.Fatalf("Call after Close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Call")
	}
	close(release)
	cleanup()
}

func peerPair(t *testing.T) (*streammux.Peer, *streammux.Peer, func()) {
	return peerPairConfigs(t, streammux.DefaultPeerConfig(), streammux.DefaultPeerConfig())
}

func peerPairConfigs(t *testing.T, clientConfig, serverConfig streammux.PeerConfig) (*streammux.Peer, *streammux.Peer, func()) {
	t.Helper()
	left, right := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	leftConn, err := streammux.Open(ctx, left, streammux.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	rightConn, err := streammux.Open(ctx, right, streammux.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	client, err := streammux.NewPeer(leftConn, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	server, err := streammux.NewPeer(rightConn, serverConfig)
	if err != nil {
		t.Fatal(err)
	}
	go client.Serve(ctx)
	go server.Serve(ctx)
	return client, server, func() { cancel(); _ = client.Close(); _ = server.Close() }
}
