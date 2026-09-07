package pty_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aruzen/streammux"
	"github.com/aruzen/streammux/pty"
)

type orderedSink struct {
	manager *pty.Manager
	id      streammux.StreamID
	events  chan pty.AttachmentEvent
}

type failingSink struct{ err error }

func (s failingSink) Send(context.Context, pty.AttachmentEvent) error { return s.err }

func (s *orderedSink) Send(_ context.Context, event pty.AttachmentEvent) error {
	copyEvent := event
	if event.Output != nil {
		output := *event.Output
		output.Data = append([]byte(nil), output.Data...)
		copyEvent.Output = &output
	}
	if event.Exit != nil {
		exit := *event.Exit
		copyEvent.Exit = &exit
		if _, ok := s.manager.Get(s.id); !ok {
			return errors.New("session removed before exit delivery")
		}
	}
	s.events <- copyEvent
	return nil
}

func TestSessionExitDrainsOutputThenExitBeforeRemoval(t *testing.T) {
	factory := &managedTestFactory{}
	manager, err := pty.NewManager(context.Background(), factory, pty.DefaultManagerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	subscription, err := manager.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	session, err := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	if err != nil {
		t.Fatal(err)
	}
	assertManagerEventKind(t, subscription.Events(), pty.EventOpened)
	sink := &orderedSink{manager: manager, id: session.ID(), events: make(chan pty.AttachmentEvent, 2)}
	attachment, _, err := session.Attach(context.Background(), pty.AttachOptions{}, sink)
	if err != nil {
		t.Fatal(err)
	}
	assertManagerEventKind(t, subscription.Events(), pty.EventAttached)
	go factory.process(0).write("tail")
	waitSessionSequence(t, session, 1)
	factory.process(0).finish(pty.ExitStatus{Reason: pty.ExitReasonExited, Code: 9})

	first := <-sink.events
	second := <-sink.events
	if first.Kind != pty.AttachmentOutput || first.Output == nil || string(first.Output.Data) != "tail" {
		t.Fatalf("first attachment event = %#v", first)
	}
	if second.Kind != pty.AttachmentExit || second.Exit == nil || second.Exit.Code != 9 {
		t.Fatalf("second attachment event = %#v", second)
	}
	if _, err = session.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertManagerEventKind(t, subscription.Events(), pty.EventOutput)
	assertManagerEventKind(t, subscription.Events(), pty.EventExited)
	assertManagerEventKind(t, subscription.Events(), pty.EventDetached)
	assertManagerEventKind(t, subscription.Events(), pty.EventRemoved)
	if _, ok := manager.Get(session.ID()); ok {
		t.Fatal("session remained registered after attachment drained")
	}
	if attachment.Reason() != pty.AttachmentSessionExited {
		t.Fatalf("attachment reason = %q", attachment.Reason())
	}
	stats := manager.Stats()
	if stats.ProcessesStarted != 1 || stats.ProcessesExited != 1 || stats.AttachmentClosures[pty.AttachmentSessionExited] != 1 {
		t.Fatalf("manager stats = %#v", stats)
	}
}

func TestPausedAttachmentKeepsReplayAndLiveContiguous(t *testing.T) {
	factory := &managedTestFactory{}
	manager, _ := pty.NewManager(context.Background(), factory, pty.DefaultManagerConfig())
	defer manager.Close()
	session, _ := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	go factory.process(0).write("history")
	waitSessionSequence(t, session, 1)
	sink := &orderedSink{manager: manager, id: session.ID(), events: make(chan pty.AttachmentEvent, 3)}
	attachment, result, err := session.Attach(context.Background(), pty.AttachOptions{Replay: pty.ReplayHistory, Paused: true}, sink)
	if err != nil {
		t.Fatal(err)
	}
	go factory.process(0).write("live")
	waitSessionSequence(t, session, 2)
	factory.process(0).finish(pty.ExitStatus{Reason: pty.ExitReasonExited, Code: 4})
	if result.ReplayFirst != 1 || result.ReplayLast != 1 || result.NextLive != 2 {
		t.Fatalf("attach result = %#v", result)
	}
	select {
	case event := <-sink.events:
		t.Fatalf("paused attachment delivered before activation: %#v", event)
	case <-time.After(20 * time.Millisecond):
	}
	if err = attachment.Activate(); err != nil {
		t.Fatal(err)
	}
	for index, want := range []string{"history", "live"} {
		event := <-sink.events
		if event.Kind != pty.AttachmentOutput || event.Output == nil || event.Output.Sequence != uint64(index+1) || string(event.Output.Data) != want {
			t.Fatalf("output %d = %#v", index, event)
		}
	}
	if event := <-sink.events; event.Kind != pty.AttachmentExit || event.Exit == nil || event.Exit.Code != 4 {
		t.Fatalf("exit event = %#v", event)
	}
	if _, err = session.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestManagerDrainsLargeOutputWithoutAttachment(t *testing.T) {
	factory := &managedTestFactory{}
	config := pty.DefaultManagerConfig()
	config.HistoryBytes = 0
	config.MaxTotalHistoryBytes = 0
	manager, _ := pty.NewManager(context.Background(), factory, config)
	defer manager.Close()
	session, _ := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	data := make([]byte, 2<<20)
	written := make(chan error, 1)
	go func() {
		_, err := factory.process(0).outputW.Write(data)
		written <- err
	}()
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unattached PTY output was not drained")
	}
	if session.Info().LastSequence == 0 {
		t.Fatal("drained output was not sequenced")
	}
}

func TestManagerLifecycleRaces(t *testing.T) {
	for range 25 {
		factory := &managedTestFactory{}
		config := pty.DefaultManagerConfig()
		config.GracefulKillTimeout = 0
		manager, _ := pty.NewManager(context.Background(), factory, config)
		session, err := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
		if err != nil {
			t.Fatal(err)
		}
		var wait sync.WaitGroup
		wait.Add(3)
		go func() { defer wait.Done(); _ = session.Kill(context.Background()) }()
		go func() { defer wait.Done(); _, _ = session.Wait(context.Background()) }()
		go func() { defer wait.Done(); _ = manager.Close() }()
		wait.Wait()
	}
}

func TestManagerRecordsTruncatedBytesAndCloseReasons(t *testing.T) {
	factory := &managedTestFactory{}
	config := pty.DefaultManagerConfig()
	config.HistoryBytes = 4
	config.MaxTotalHistoryBytes = 4
	manager, _ := pty.NewManager(context.Background(), factory, config)
	defer manager.Close()
	subscription, err := manager.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	if err = subscription.Close(); err != nil {
		t.Fatal(err)
	}
	session, _ := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	attachment, _, err := session.Attach(context.Background(), pty.AttachOptions{}, failingSink{err: errors.New("sink failed")})
	if err != nil {
		t.Fatal(err)
	}
	go factory.process(0).write("0123456789")
	waitSessionSequence(t, session, 1)
	select {
	case <-attachment.Done():
	case <-time.After(time.Second):
		t.Fatal("failing attachment did not close")
	}
	stats := manager.Stats()
	if stats.HistoryTruncatedBytes != 6 {
		t.Fatalf("truncated bytes = %d, want 6", stats.HistoryTruncatedBytes)
	}
	if stats.AttachmentClosures[pty.AttachmentSinkFailure] != 1 || stats.ObserverClosures[pty.ObserverClosedExplicitly] != 1 {
		t.Fatalf("closure counters = %#v, %#v", stats.AttachmentClosures, stats.ObserverClosures)
	}
}

func TestSessionExitTimesOutBlockedAttachmentDrain(t *testing.T) {
	factory := &managedTestFactory{}
	config := pty.DefaultManagerConfig()
	config.AttachmentDrainTimeout = 20 * time.Millisecond
	manager, _ := pty.NewManager(context.Background(), factory, config)
	defer manager.Close()
	session, _ := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	sink := &blockingOutput{started: make(chan struct{})}
	attachment, _, err := session.Attach(context.Background(), pty.AttachOptions{}, sink)
	if err != nil {
		t.Fatal(err)
	}
	go factory.process(0).write("blocked")
	select {
	case <-sink.started:
	case <-time.After(time.Second):
		t.Fatal("sink did not start")
	}
	factory.process(0).finish(pty.ExitStatus{Reason: pty.ExitReasonExited})
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err = session.Wait(waitCtx); err != nil {
		t.Fatalf("Session.Wait remained blocked: %v", err)
	}
	if attachment.Reason() != pty.AttachmentDrainTimeout || !errors.Is(attachment.Err(), context.DeadlineExceeded) {
		t.Fatalf("attachment close = %q, %v", attachment.Reason(), attachment.Err())
	}
}
