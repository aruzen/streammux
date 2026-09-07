package pty_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/aruzen/streammux"
	"github.com/aruzen/streammux/pty"
)

var protocolTypes = pty.MessageTypes{Open: 50, List: 51, Inspect: 52, Attach: 53, Detach: 54, Input: 55, Output: 56, Resize: 57, Kill: 58, Exit: 59, ReplayBegin: 60, ReplayEnd: 61, Error: 62}

type protocolReceived struct {
	kind streammux.MessageType
	data string
}

func TestProtocolOpenAttachReplayDetachAndKill(t *testing.T) {
	left, right := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientConn, _ := streammux.Open(ctx, left, streammux.DefaultConfig())
	serverConn, _ := streammux.Open(ctx, right, streammux.DefaultConfig())
	clientPeer, _ := streammux.NewPeer(clientConn, streammux.DefaultPeerConfig())
	serverPeer, _ := streammux.NewPeer(serverConn, streammux.DefaultPeerConfig())
	factory := &managedTestFactory{}
	managerConfig := pty.DefaultManagerConfig()
	managerConfig.MaxAttachmentsPerSession = 2
	manager, _ := pty.NewManager(context.Background(), factory, managerConfig)
	defer manager.Close()
	protocol, err := pty.RegisterProtocol(serverPeer, manager, pty.ProtocolConfig{Version: 1, Types: protocolTypes, MaxListItems: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer protocol.Close()
	events := make(chan protocolReceived, 16)
	for _, messageType := range []streammux.MessageType{protocolTypes.ReplayBegin, protocolTypes.ReplayEnd, protocolTypes.Output, protocolTypes.Exit} {
		mt := messageType
		if err = clientPeer.Register(mt, func(_ context.Context, _ *streammux.Peer, frame streammux.Frame) error {
			events <- protocolReceived{mt, string(frame.Payload)}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	go clientPeer.Serve(ctx)
	go serverPeer.Serve(ctx)

	openPayload, _ := json.Marshal(map[string]any{"version": 1, "command": "shell", "initial_size": map[string]int{"Cols": 100, "Rows": 30}})
	openRequest := requestFrame(t, protocolTypes.Open, 0, openPayload)
	openResponse, err := clientPeer.Call(context.Background(), openRequest)
	if err != nil {
		t.Fatal(err)
	}
	var opened struct {
		Session pty.SessionInfo  `json:"session"`
		Error   *pty.RemoteError `json:"error"`
	}
	if err = json.Unmarshal(openResponse.Payload, &opened); err != nil || opened.Error != nil || opened.Session.ID == 0 {
		t.Fatalf("Open response = %s, %v", openResponse.Payload, err)
	}
	go factory.process(0).write("retained")
	session, _ := manager.Get(opened.Session.ID)
	waitSessionSequence(t, session, 1)

	attachPayload, _ := json.Marshal(map[string]any{"version": 1, "replay": "history"})
	attachResponse, err := clientPeer.Call(context.Background(), requestFrame(t, protocolTypes.Attach, opened.Session.ID, attachPayload))
	if err != nil {
		t.Fatal(err)
	}
	var attached struct {
		Attach pty.AttachResult `json:"attach"`
		Error  *pty.RemoteError `json:"error"`
	}
	if err = json.Unmarshal(attachResponse.Payload, &attached); err != nil || attached.Error != nil || attached.Attach.ReplayFirst != 1 {
		t.Fatalf("Attach response = %s, %v", attachResponse.Payload, err)
	}
	assertProtocolEvent(t, events, protocolTypes.ReplayBegin)
	output := assertProtocolEvent(t, events, protocolTypes.Output)
	if output.data != "retained" {
		t.Fatalf("replay output = %q", output.data)
	}
	assertProtocolEvent(t, events, protocolTypes.ReplayEnd)

	if _, err = clientPeer.Call(context.Background(), requestFrame(t, protocolTypes.Detach, opened.Session.ID, nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-factory.process(0).done:
		t.Fatal("Detach killed process")
	default:
	}
	if _, err = clientPeer.Call(context.Background(), requestFrame(t, protocolTypes.Attach, opened.Session.ID, attachPayload)); err != nil {
		t.Fatal(err)
	}
	assertProtocolEvent(t, events, protocolTypes.ReplayBegin)
	assertProtocolEvent(t, events, protocolTypes.Output)
	assertProtocolEvent(t, events, protocolTypes.ReplayEnd)
	go factory.process(0).write("final-output")
	waitSessionSequence(t, session, 2)
	if _, err = clientPeer.Call(context.Background(), requestFrame(t, protocolTypes.Kill, opened.Session.ID, nil)); err != nil {
		t.Fatal(err)
	}
	if output = assertProtocolEvent(t, events, protocolTypes.Output); output.data != "final-output" {
		t.Fatalf("final output = %q", output.data)
	}
	assertProtocolEvent(t, events, protocolTypes.Exit)
	if _, ok := manager.Get(opened.Session.ID); ok {
		t.Fatal("default lifecycle retained session after Exit delivery")
	}
}

func TestProtocolOperationErrorKeepsPeerUsable(t *testing.T) {
	left, right := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientConn, _ := streammux.Open(ctx, left, streammux.DefaultConfig())
	serverConn, _ := streammux.Open(ctx, right, streammux.DefaultConfig())
	clientPeer, _ := streammux.NewPeer(clientConn, streammux.DefaultPeerConfig())
	serverPeer, _ := streammux.NewPeer(serverConn, streammux.DefaultPeerConfig())
	factory := &managedTestFactory{}
	managerConfig := pty.DefaultManagerConfig()
	manager, _ := pty.NewManager(context.Background(), factory, managerConfig)
	defer manager.Close()
	protocol, _ := pty.RegisterProtocol(serverPeer, manager, pty.ProtocolConfig{Version: 1, Types: protocolTypes, MaxListItems: 8})
	defer protocol.Close()
	go clientPeer.Serve(ctx)
	go serverPeer.Serve(ctx)
	response, err := clientPeer.Call(context.Background(), requestFrame(t, protocolTypes.Inspect, 999, nil))
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Error *pty.RemoteError `json:"error"`
	}
	if err = json.Unmarshal(response.Payload, &envelope); err != nil || envelope.Error == nil || envelope.Error.Code != pty.CodeSessionNotFound {
		t.Fatalf("error response = %s", response.Payload)
	}
	listPayload, _ := json.Marshal(map[string]any{"version": 1})
	response, err = clientPeer.Call(context.Background(), requestFrame(t, protocolTypes.List, 0, listPayload))
	if err != nil {
		t.Fatalf("Peer closed after operation error: %v", err)
	}
}

func TestProtocolConnectionCloseDetachesWithoutKillingAndAllowsReattach(t *testing.T) {
	factory := &managedTestFactory{}
	manager, err := pty.NewManager(context.Background(), factory, pty.DefaultManagerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	client1, server1, close1 := protocolPeerPair(t, manager)
	openPayload, _ := pty.EncodeControl(pty.OpenRequest{Version: 1, Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	response, err := client1.Call(context.Background(), requestFrame(t, protocolTypes.Open, 0, openPayload))
	if err != nil {
		t.Fatal(err)
	}
	opened, err := pty.DecodeProtocolResponse(response.Payload)
	if err != nil || opened.Session == nil {
		t.Fatalf("Open response = %#v, %v", opened, err)
	}
	id := opened.Session.ID
	close1()
	select {
	case <-factory.process(0).done:
		t.Fatal("connection close killed process")
	default:
	}

	go factory.process(0).write("while-disconnected")
	session, ok := manager.Get(id)
	if !ok {
		t.Fatal("session disappeared with connection")
	}
	waitSessionSequence(t, session, 1)

	client2, server2, close2 := protocolPeerPair(t, manager)
	defer close2()
	events := make(chan protocolReceived, 4)
	for _, messageType := range []streammux.MessageType{protocolTypes.ReplayBegin, protocolTypes.Output, protocolTypes.ReplayEnd} {
		mt := messageType
		if err = client2.Register(mt, func(_ context.Context, _ *streammux.Peer, frame streammux.Frame) error {
			events <- protocolReceived{kind: mt, data: string(frame.Payload)}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	attachPayload, _ := pty.EncodeControl(pty.AttachRequest{Version: 1, Replay: pty.ReplayHistory})
	if _, err = client2.Call(context.Background(), requestFrame(t, protocolTypes.Attach, id, attachPayload)); err != nil {
		t.Fatal(err)
	}
	assertProtocolEvent(t, events, protocolTypes.ReplayBegin)
	if output := assertProtocolEvent(t, events, protocolTypes.Output); output.data != "while-disconnected" {
		t.Fatalf("replayed output = %q", output.data)
	}
	end := assertProtocolEvent(t, events, protocolTypes.ReplayEnd)
	if end.data != "" {
		t.Fatalf("ReplayEnd payload = %q, want empty", end.data)
	}
	_ = server1
	_ = server2
}

func TestProtocolAttachesToRetainedExitedSession(t *testing.T) {
	factory := &managedTestFactory{}
	manager, err := pty.NewManager(context.Background(), factory, retentionConfig(2))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	session, err := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	if err != nil {
		t.Fatal(err)
	}
	go factory.process(0).write("final-history")
	waitSessionSequence(t, session, 1)
	factory.process(0).finish(pty.ExitStatus{Reason: pty.ExitReasonExited, Code: 12})
	if _, err = session.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	client, _, closePair := protocolPeerPair(t, manager)
	defer closePair()
	resizePayload, err := pty.EncodeSize(pty.Size{Cols: 100, Rows: 40})
	if err != nil {
		t.Fatal(err)
	}
	resizeResponse, err := client.Call(context.Background(), requestFrame(t, protocolTypes.Resize, session.ID(), resizePayload))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pty.DecodeProtocolResponse(resizeResponse.Payload); err == nil {
		t.Fatal("Resize on exited Session succeeded")
	} else {
		var remote *pty.RemoteError
		if !errors.As(err, &remote) || remote.Code != pty.CodeSessionExited {
			t.Fatalf("Resize error = %v", err)
		}
	}
	events := make(chan protocolReceived, 4)
	for _, messageType := range []streammux.MessageType{protocolTypes.ReplayBegin, protocolTypes.Output, protocolTypes.ReplayEnd, protocolTypes.Exit} {
		mt := messageType
		if err = client.Register(mt, func(_ context.Context, _ *streammux.Peer, frame streammux.Frame) error {
			events <- protocolReceived{kind: mt, data: string(frame.Payload)}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	payload, _ := pty.EncodeControl(pty.AttachRequest{Version: 1, Replay: pty.ReplayHistory})
	response, err := client.Call(context.Background(), requestFrame(t, protocolTypes.Attach, session.ID(), payload))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := pty.DecodeProtocolResponse(response.Payload)
	if err != nil || decoded.Attach == nil || decoded.Attach.ReplayFirst != 1 || decoded.Attach.ReplayLast != 1 {
		t.Fatalf("Attach response = %#v, %v", decoded, err)
	}
	assertProtocolEvent(t, events, protocolTypes.ReplayBegin)
	if output := assertProtocolEvent(t, events, protocolTypes.Output); output.data != "final-history" {
		t.Fatalf("replayed output = %q", output.data)
	}
	assertProtocolEvent(t, events, protocolTypes.ReplayEnd)
	exit := assertProtocolEvent(t, events, protocolTypes.Exit)
	var exitEnvelope struct {
		Exit pty.ExitStatus `json:"exit"`
	}
	if err = json.Unmarshal([]byte(exit.data), &exitEnvelope); err != nil || exitEnvelope.Exit.Code != 12 {
		t.Fatalf("Exit event = %s, %v", exit.data, err)
	}
}

func TestDecodeProtocolResponseReturnsStructuredError(t *testing.T) {
	payload, err := pty.EncodeControl(pty.ProtocolResponse{Version: 1, Error: &pty.RemoteError{Code: pty.CodeSessionNotFound, Message: "gone"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = pty.DecodeProtocolResponse(payload)
	var remote *pty.RemoteError
	if !errors.As(err, &remote) || remote.Code != pty.CodeSessionNotFound {
		t.Fatalf("DecodeProtocolResponse error = %v", err)
	}
}

func protocolPeerPair(t *testing.T, manager *pty.Manager) (*streammux.Peer, *streammux.Peer, func()) {
	t.Helper()
	left, right := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	clientConn, err := streammux.Open(ctx, left, streammux.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	serverConn, err := streammux.Open(ctx, right, streammux.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	client, err := streammux.NewPeer(clientConn, streammux.DefaultPeerConfig())
	if err != nil {
		t.Fatal(err)
	}
	server, err := streammux.NewPeer(serverConn, streammux.DefaultPeerConfig())
	if err != nil {
		t.Fatal(err)
	}
	protocol, err := pty.RegisterProtocol(server, manager, pty.ProtocolConfig{Version: 1, Types: protocolTypes, MaxListItems: 8})
	if err != nil {
		t.Fatal(err)
	}
	go client.Serve(ctx)
	go server.Serve(ctx)
	return client, server, func() {
		cancel()
		_ = client.Close()
		_ = server.Close()
		_ = protocol.Close()
	}
}

func requestFrame(t *testing.T, messageType streammux.MessageType, streamID streammux.StreamID, payload []byte) streammux.Frame {
	t.Helper()
	frame, err := streammux.NewFrame(streammux.Header{Version: 1, MessageType: messageType, Flags: streammux.FlagRequest, CorrelationID: 1, StreamID: streamID}, payload)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}
func assertProtocolEvent(t *testing.T, events <-chan protocolReceived, want streammux.MessageType) protocolReceived {
	t.Helper()
	select {
	case event := <-events:
		if event.kind != want {
			t.Fatalf("event type = %d, want %d", event.kind, want)
		}
		return event
	case <-time.After(time.Second):
		t.Fatalf("event %d timed out", want)
		return protocolReceived{}
	}
}
