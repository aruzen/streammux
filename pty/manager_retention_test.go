package pty_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aruzen/streammux/pty"
)

type attachmentCollector struct {
	events chan pty.AttachmentEvent
}

func retentionConfig(maxSessions int) pty.ManagerConfig {
	config := pty.DefaultManagerConfig()
	config.SessionLifecycle = pty.SessionLifecycleDescriptor{
		ExitPolicy:          pty.ExitedSessionRetain,
		MaxRetainedSessions: maxSessions,
	}
	return config
}

func (s *attachmentCollector) Send(_ context.Context, event pty.AttachmentEvent) error {
	copyEvent := event
	if event.Output != nil {
		output := *event.Output
		output.Data = append([]byte(nil), output.Data...)
		copyEvent.Output = &output
	}
	if event.Exit != nil {
		exit := *event.Exit
		copyEvent.Exit = &exit
	}
	s.events <- copyEvent
	return nil
}

func TestRetainedExitedSessionCanReplayAndBeRemoved(t *testing.T) {
	factory := &managedTestFactory{}
	config := retentionConfig(2)
	manager, err := pty.NewManager(context.Background(), factory, config)
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

	go factory.process(0).write("retained-output")
	waitSessionSequence(t, session, 1)
	factory.process(0).finish(pty.ExitStatus{Reason: pty.ExitReasonExited, Code: 9})
	status, err := session.Wait(context.Background())
	if err != nil || status.Code != 9 {
		t.Fatalf("Wait = %#v, %v", status, err)
	}
	assertManagerEventKind(t, subscription.Events(), pty.EventOutput)
	assertManagerEventKind(t, subscription.Events(), pty.EventExited)
	assertManagerEventKind(t, subscription.Events(), pty.EventRetained)

	info := session.Info()
	if info.State != pty.SessionExited || info.Exit == nil || info.Exit.Code != 9 || !info.HistoryAvailable || info.AttachmentCount != 0 {
		t.Fatalf("retained SessionInfo = %#v", info)
	}
	if err = session.Input(context.Background(), []byte("x")); !errors.Is(err, pty.ErrSessionExited) {
		t.Fatalf("Input after exit = %v", err)
	}
	if err = session.Resize(context.Background(), pty.Size{Cols: 100, Rows: 40}); !errors.Is(err, pty.ErrSessionExited) {
		t.Fatalf("Resize after exit = %v", err)
	}

	sink := &attachmentCollector{events: make(chan pty.AttachmentEvent, 2)}
	attachment, result, err := session.Attach(context.Background(), pty.AttachOptions{Replay: pty.ReplayHistory}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if result.ReplayFirst != 1 || result.ReplayLast != 1 || result.NextLive != 2 || result.Truncated {
		t.Fatalf("AttachResult = %#v", result)
	}
	attached := assertManagerEventKind(t, subscription.Events(), pty.EventAttached)
	if attached.Session.AttachmentCount != 1 || attached.Attachment == nil || attached.Attachment.ID != attachment.ID() {
		t.Fatalf("attached event = %#v", attached)
	}
	assertAttachmentEvent(t, sink.events, pty.AttachmentOutput, "retained-output", 9)
	assertAttachmentEvent(t, sink.events, pty.AttachmentExit, "", 9)
	select {
	case <-attachment.Done():
	case <-time.After(time.Second):
		t.Fatal("retained attachment did not finish")
	}
	detached := assertManagerEventKind(t, subscription.Events(), pty.EventDetached)
	if detached.Attachment == nil || detached.Attachment.ID != attachment.ID() || detached.Attachment.Reason != pty.AttachmentSessionExited {
		t.Fatalf("detached event = %#v", detached)
	}
	if detached.Session.AttachmentCount != 0 {
		t.Fatalf("detached AttachmentCount = %d", detached.Session.AttachmentCount)
	}

	if err = manager.Remove(session.ID()); err != nil {
		t.Fatal(err)
	}
	removed := assertManagerEventKind(t, subscription.Events(), pty.EventRemoved)
	if removed.Session.HistoryAvailable {
		t.Fatalf("removed event retained history: %#v", removed)
	}
	if _, ok := manager.Get(session.ID()); ok {
		t.Fatal("removed session remained registered")
	}
	stats := manager.Stats()
	if stats.RetainedExitedSessions != 0 || stats.SessionsRemoved != 1 {
		t.Fatalf("ManagerStats = %#v", stats)
	}
}

func TestRetainedSessionLimitEvictsOldest(t *testing.T) {
	factory := &managedTestFactory{}
	config := retentionConfig(2)
	manager, err := pty.NewManager(context.Background(), factory, config)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	var sessions []*pty.Session
	for index := 0; index < 3; index++ {
		session, openErr := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
		if openErr != nil {
			t.Fatal(openErr)
		}
		sessions = append(sessions, session)
		factory.process(index).finish(pty.ExitStatus{Reason: pty.ExitReasonExited, Code: index + 1})
		if _, openErr = session.Wait(context.Background()); openErr != nil {
			t.Fatal(openErr)
		}
	}
	if _, ok := manager.Get(sessions[0].ID()); ok {
		t.Fatal("oldest retained session was not evicted")
	}
	for _, session := range sessions[1:] {
		if _, ok := manager.Get(session.ID()); !ok {
			t.Fatalf("newer retained session %d was evicted", session.ID())
		}
	}
	stats := manager.Stats()
	if stats.RetainedExitedSessions != 2 || stats.SessionsEvicted != 1 || stats.Sessions != 2 {
		t.Fatalf("ManagerStats = %#v", stats)
	}
}

func TestRetentionEvictionDetachesActiveReplay(t *testing.T) {
	factory := &managedTestFactory{}
	config := retentionConfig(1)
	manager, err := pty.NewManager(context.Background(), factory, config)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	oldest, err := manager.Open(context.Background(), pty.ProcessSpec{Command: "oldest", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	if err != nil {
		t.Fatal(err)
	}
	go factory.process(0).write("history")
	waitSessionSequence(t, oldest, 1)
	factory.process(0).finish(pty.ExitStatus{Reason: pty.ExitReasonExited, Code: 1})
	if _, err = oldest.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	subscription, err := manager.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	sink := &blockingOutput{started: make(chan struct{})}
	attachment, _, err := oldest.Attach(context.Background(), pty.AttachOptions{Replay: pty.ReplayHistory}, sink)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-sink.started:
	case <-time.After(time.Second):
		t.Fatal("retained replay did not start")
	}
	assertManagerEventKind(t, subscription.Events(), pty.EventAttached)

	newest, err := manager.Open(context.Background(), pty.ProcessSpec{Command: "newest", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	if err != nil {
		t.Fatal(err)
	}
	factory.process(1).finish(pty.ExitStatus{Reason: pty.ExitReasonExited, Code: 2})
	if _, err = newest.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertManagerEventKind(t, subscription.Events(), pty.EventOpened)
	assertManagerEventKind(t, subscription.Events(), pty.EventExited)
	assertManagerEventKind(t, subscription.Events(), pty.EventRetained)
	detached := assertManagerEventKind(t, subscription.Events(), pty.EventDetached)
	if detached.Attachment == nil || detached.Attachment.Reason != pty.AttachmentSessionEvicted {
		t.Fatalf("detached event = %#v", detached)
	}
	assertManagerEventKind(t, subscription.Events(), pty.EventEvicted)
	select {
	case <-attachment.Done():
	case <-time.After(time.Second):
		t.Fatal("retention eviction did not stop active replay")
	}
	if attachment.Reason() != pty.AttachmentSessionEvicted {
		t.Fatalf("attachment reason = %q", attachment.Reason())
	}
	if _, ok := manager.Get(oldest.ID()); ok {
		t.Fatal("evicted Session remained registered")
	}
	if _, ok := manager.Get(newest.ID()); !ok {
		t.Fatal("newest retained Session was evicted")
	}
}

func TestDefaultLifecycleRemovesSessionAfterExit(t *testing.T) {
	factory := &managedTestFactory{}
	config := pty.DefaultManagerConfig()
	manager, err := pty.NewManager(context.Background(), factory, config)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	session, err := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	if err != nil {
		t.Fatal(err)
	}
	factory.process(0).finish(pty.ExitStatus{Reason: pty.ExitReasonExited})
	if _, err = session.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.Get(session.ID()); ok {
		t.Fatal("zero-retention Session remained registered")
	}
	stats := manager.Stats()
	if stats.RetainedExitedSessions != 0 || stats.SessionsRemoved != 1 {
		t.Fatalf("ManagerStats = %#v", stats)
	}
}

func TestLifecycleDescriptorValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*pty.ManagerConfig)
	}{
		{
			name: "remove with retention limit",
			mutate: func(config *pty.ManagerConfig) {
				config.SessionLifecycle.MaxRetainedSessions = 1
			},
		},
		{
			name: "retain without limit",
			mutate: func(config *pty.ManagerConfig) {
				config.SessionLifecycle.ExitPolicy = pty.ExitedSessionRetain
			},
		},
		{
			name: "unknown policy",
			mutate: func(config *pty.ManagerConfig) {
				config.SessionLifecycle.ExitPolicy = pty.ExitedSessionPolicy(255)
			},
		},
		{
			name: "unbounded attachments",
			mutate: func(config *pty.ManagerConfig) {
				config.MaxAttachmentsPerSession = 0
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := pty.DefaultManagerConfig()
			test.mutate(&config)
			if _, err := pty.NewManager(context.Background(), &managedTestFactory{}, config); err == nil {
				t.Fatal("NewManager accepted invalid lifecycle configuration")
			}
		})
	}
}

func TestConcurrentAttachHonorsSessionLimitAtomically(t *testing.T) {
	factory := &managedTestFactory{}
	config := pty.DefaultManagerConfig()
	config.MaxAttachmentsPerSession = 1
	manager, err := pty.NewManager(context.Background(), factory, config)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	session, err := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	if err != nil {
		t.Fatal(err)
	}

	const attempts = 16
	start := make(chan struct{})
	results := make(chan error, attempts)
	attachments := make(chan *pty.Attachment, attempts)
	var wait sync.WaitGroup
	wait.Add(attempts)
	for range attempts {
		go func() {
			defer wait.Done()
			<-start
			sink := &attachmentCollector{events: make(chan pty.AttachmentEvent, 1)}
			attachment, _, attachErr := session.Attach(context.Background(), pty.AttachOptions{}, sink)
			if attachErr == nil {
				attachments <- attachment
			}
			results <- attachErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(attachments)

	successes := 0
	limits := 0
	for result := range results {
		switch {
		case result == nil:
			successes++
		case errors.Is(result, pty.ErrAttachmentLimit):
			limits++
		default:
			t.Fatalf("Attach result = %v", result)
		}
	}
	if successes != 1 || limits != attempts-1 {
		t.Fatalf("Attach results: success=%d limit=%d", successes, limits)
	}
	for attachment := range attachments {
		_ = attachment.Close()
	}
}

func TestRetainedSessionsDoNotConsumeActiveSessionLimit(t *testing.T) {
	factory := &managedTestFactory{}
	config := retentionConfig(2)
	config.MaxSessions = 1
	manager, err := pty.NewManager(context.Background(), factory, config)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	first, err := manager.Open(context.Background(), pty.ProcessSpec{Command: "first", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	if err != nil {
		t.Fatal(err)
	}
	factory.process(0).finish(pty.ExitStatus{Reason: pty.ExitReasonExited})
	if _, err = first.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := manager.Open(context.Background(), pty.ProcessSpec{Command: "second", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	if err != nil {
		t.Fatalf("Open with one retained Session = %v", err)
	}
	if stats := manager.Stats(); stats.Sessions != 2 || stats.ActiveSessions != 1 || stats.RetainedExitedSessions != 1 {
		t.Fatalf("ManagerStats with active and retained Sessions = %#v", stats)
	}
	if _, err = manager.Open(context.Background(), pty.ProcessSpec{Command: "third", InitialSize: pty.Size{Cols: 80, Rows: 24}}); !errors.Is(err, pty.ErrSessionLimit) {
		t.Fatalf("Open over active limit = %v", err)
	}
	factory.process(1).finish(pty.ExitStatus{Reason: pty.ExitReasonExited})
	if _, err = second.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stats := manager.Stats(); stats.Sessions != 2 || stats.ActiveSessions != 0 || stats.RetainedExitedSessions != 2 {
		t.Fatalf("ManagerStats = %#v", stats)
	}
}

func TestHistoryAvailableTracksReplayableChunks(t *testing.T) {
	factory := &managedTestFactory{}
	config := retentionConfig(2)
	config.HistoryBytes = 3
	config.MaxTotalHistoryBytes = 3
	manager, err := pty.NewManager(context.Background(), factory, config)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	first, _ := manager.Open(context.Background(), pty.ProcessSpec{Command: "first", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	go factory.process(0).write("one")
	waitSessionSequence(t, first, 1)
	factory.process(0).finish(pty.ExitStatus{Reason: pty.ExitReasonExited})
	if _, err = first.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !first.Info().HistoryAvailable {
		t.Fatal("retained history was not reported available")
	}

	second, _ := manager.Open(context.Background(), pty.ProcessSpec{Command: "second", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	go factory.process(1).write("two")
	waitSessionSequence(t, second, 1)
	if first.Info().HistoryAvailable {
		t.Fatal("globally evicted history remained available")
	}
	factory.process(1).finish(pty.ExitStatus{Reason: pty.ExitReasonExited})
	_, _ = second.Wait(context.Background())
}

func TestManagerCloseReleasesRetainedSessionHistory(t *testing.T) {
	factory := &managedTestFactory{}
	manager, err := pty.NewManager(context.Background(), factory, retentionConfig(2))
	if err != nil {
		t.Fatal(err)
	}
	session, err := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	if err != nil {
		t.Fatal(err)
	}
	go factory.process(0).write("history")
	waitSessionSequence(t, session, 1)
	factory.process(0).finish(pty.ExitStatus{Reason: pty.ExitReasonExited, Code: 1})
	if _, err = session.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stats := manager.Stats(); stats.RetainedExitedSessions != 1 || stats.HistoryBytes == 0 {
		t.Fatalf("ManagerStats before Close = %#v", stats)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.Get(session.ID()); ok {
		t.Fatal("retained Session remained after Manager.Close")
	}
	if stats := manager.Stats(); stats.Sessions != 0 || stats.RetainedExitedSessions != 0 || stats.HistoryBytes != 0 {
		t.Fatalf("ManagerStats after Close = %#v", stats)
	}
}

func TestRemoveRejectsRunningSession(t *testing.T) {
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
	if err = manager.Remove(session.ID()); !errors.Is(err, pty.ErrSessionRunning) {
		t.Fatalf("Remove running Session = %v", err)
	}
}

func TestRemoveDetachesActiveRetainedAttachment(t *testing.T) {
	factory := &managedTestFactory{}
	manager, err := pty.NewManager(context.Background(), factory, retentionConfig(2))
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
	go factory.process(0).write("history")
	waitSessionSequence(t, session, 1)
	factory.process(0).finish(pty.ExitStatus{Reason: pty.ExitReasonExited, Code: 3})
	if _, err = session.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertManagerEventKind(t, subscription.Events(), pty.EventOutput)
	assertManagerEventKind(t, subscription.Events(), pty.EventExited)
	assertManagerEventKind(t, subscription.Events(), pty.EventRetained)

	sink := &blockingOutput{started: make(chan struct{})}
	attachment, _, err := session.Attach(context.Background(), pty.AttachOptions{Replay: pty.ReplayHistory}, sink)
	if err != nil {
		t.Fatal(err)
	}
	assertManagerEventKind(t, subscription.Events(), pty.EventAttached)
	select {
	case <-sink.started:
	case <-time.After(time.Second):
		t.Fatal("retained replay did not start")
	}
	if err = manager.Remove(session.ID()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-attachment.Done():
	case <-time.After(time.Second):
		t.Fatal("Remove did not stop retained attachment")
	}
	detached := assertManagerEventKind(t, subscription.Events(), pty.EventDetached)
	if detached.Attachment == nil || detached.Attachment.Reason != pty.AttachmentSessionRemoved {
		t.Fatalf("detached event = %#v", detached)
	}
	assertManagerEventKind(t, subscription.Events(), pty.EventRemoved)
}

func TestRetainedAttachRemoveAndCloseRace(t *testing.T) {
	for range 25 {
		factory := &managedTestFactory{}
		manager, err := pty.NewManager(context.Background(), factory, retentionConfig(2))
		if err != nil {
			t.Fatal(err)
		}
		session, err := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
		if err != nil {
			t.Fatal(err)
		}
		factory.process(0).finish(pty.ExitStatus{Reason: pty.ExitReasonExited})
		if _, err = session.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		errorsFound := make(chan error, 3)
		var wait sync.WaitGroup
		wait.Add(3)
		go func() {
			defer wait.Done()
			<-start
			sink := &attachmentCollector{events: make(chan pty.AttachmentEvent, 2)}
			attachment, _, attachErr := session.Attach(context.Background(), pty.AttachOptions{Replay: pty.ReplayHistory}, sink)
			if attachErr == nil {
				_ = attachment.Close()
				return
			}
			if !errors.Is(attachErr, pty.ErrSessionNotFound) && !errors.Is(attachErr, pty.ErrSessionExited) {
				errorsFound <- attachErr
			}
		}()
		go func() {
			defer wait.Done()
			<-start
			removeErr := manager.Remove(session.ID())
			if removeErr != nil && !errors.Is(removeErr, pty.ErrManagerClosed) && !errors.Is(removeErr, pty.ErrSessionNotFound) {
				errorsFound <- removeErr
			}
		}()
		go func() {
			defer wait.Done()
			<-start
			if closeErr := manager.Close(); closeErr != nil {
				errorsFound <- closeErr
			}
		}()
		close(start)
		wait.Wait()
		close(errorsFound)
		for found := range errorsFound {
			t.Fatalf("concurrent lifecycle operation = %v", found)
		}
	}
}

func assertManagerEventKind(t *testing.T, events <-chan pty.Event, kind pty.EventKind) pty.Event {
	t.Helper()
	select {
	case event := <-events:
		if event.Kind != kind {
			t.Fatalf("Manager event kind = %q, want %q: %#v", event.Kind, kind, event)
		}
		return event
	case <-time.After(time.Second):
		t.Fatalf("Manager event %q timed out", kind)
		return pty.Event{}
	}
}

func assertAttachmentEvent(t *testing.T, events <-chan pty.AttachmentEvent, kind pty.AttachmentEventKind, output string, exitCode int) {
	t.Helper()
	select {
	case event := <-events:
		if event.Kind != kind {
			t.Fatalf("attachment event kind = %q, want %q", event.Kind, kind)
		}
		switch kind {
		case pty.AttachmentOutput:
			if event.Output == nil || string(event.Output.Data) != output {
				t.Fatalf("attachment output = %#v", event)
			}
		case pty.AttachmentExit:
			if event.Exit == nil || event.Exit.Code != exitCode {
				t.Fatalf("attachment exit = %#v", event)
			}
		}
	case <-time.After(time.Second):
		t.Fatalf("attachment event %q timed out", kind)
	}
}
