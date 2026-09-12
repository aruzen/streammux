package pty_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aruzen/streammux/pty"
)

type managedTestFactory struct {
	mu        sync.Mutex
	specs     []pty.ProcessSpec
	processes []*managedTestProcess
}

type blockingManagedFactory struct {
	started chan struct{}
	once    sync.Once
}

func (f *blockingManagedFactory) StartManaged(ctx context.Context, _ pty.ProcessSpec) (pty.ManagedProcess, error) {
	f.once.Do(func() { close(f.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *managedTestFactory) StartManaged(_ context.Context, spec pty.ProcessSpec) (pty.ManagedProcess, error) {
	p := newManagedTestProcess()
	f.mu.Lock()
	f.specs = append(f.specs, spec)
	f.processes = append(f.processes, p)
	f.mu.Unlock()
	return p, nil
}
func (f *managedTestFactory) process(index int) *managedTestProcess {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.processes[index]
}

type managedTestProcess struct {
	outputR *io.PipeReader
	outputW *io.PipeWriter
	input   lockedBytes
	mu      sync.Mutex
	size    pty.Size
	status  pty.ExitStatus
	done    chan struct{}
	endOnce sync.Once
	closed  bool
}

func newManagedTestProcess() *managedTestProcess {
	r, w := io.Pipe()
	return &managedTestProcess{outputR: r, outputW: w, done: make(chan struct{})}
}
func (p *managedTestProcess) Output() io.Reader { return p.outputR }
func (p *managedTestProcess) Input() io.Writer  { return &p.input }
func (p *managedTestProcess) Resize(size pty.Size) error {
	p.mu.Lock()
	p.size = size
	p.mu.Unlock()
	return nil
}
func (p *managedTestProcess) Terminate() error {
	p.finish(pty.ExitStatus{Reason: pty.ExitReasonExited, Code: 0})
	return nil
}
func (p *managedTestProcess) Kill() error {
	p.finish(pty.ExitStatus{Reason: pty.ExitReasonKilled, Code: -1})
	return nil
}
func (p *managedTestProcess) WaitStatus() (pty.ExitStatus, error) {
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.status, nil
}
func (p *managedTestProcess) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	_ = p.outputR.Close()
	return nil
}
func (p *managedTestProcess) finish(status pty.ExitStatus) {
	p.endOnce.Do(func() { _ = p.outputW.Close(); p.mu.Lock(); p.status = status; p.mu.Unlock(); close(p.done) })
}
func (p *managedTestProcess) write(data string) { _, _ = io.WriteString(p.outputW, data) }

type lockedBytes struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *lockedBytes) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(data)
}

type collectingOutput struct{ events chan pty.OutputEvent }

func (s *collectingOutput) Send(_ context.Context, event pty.AttachmentEvent) error {
	if event.Output != nil {
		s.events <- pty.OutputEvent{Sequence: event.Output.Sequence, Data: append([]byte(nil), event.Output.Data...)}
	}
	return nil
}

type blockingOutput struct {
	started chan struct{}
	once    sync.Once
}

func (s *blockingOutput) Send(ctx context.Context, _ pty.AttachmentEvent) error {
	s.once.Do(func() { close(s.started) })
	<-ctx.Done()
	return ctx.Err()
}

func TestManagerHistoryReplayLiveAndDetach(t *testing.T) {
	factory := &managedTestFactory{}
	config := pty.DefaultManagerConfig()
	config.HistoryBytes = 64
	config.MaxTotalHistoryBytes = 128
	config.MaxAttachmentsPerSession = 2
	manager, err := pty.NewManager(context.Background(), factory, config)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	spec := pty.ProcessSpec{Command: "shell", Args: []string{"one"}, Env: []string{"A=B"}, InitialSize: pty.Size{Cols: 100, Rows: 30}}
	session, err := manager.Open(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	spec.Args[0] = "changed"
	spec.Env[0] = "changed"
	go factory.process(0).write("history")
	waitSessionSequence(t, session, 1)
	sink := &collectingOutput{events: make(chan pty.OutputEvent, 4)}
	attachment, result, err := session.Attach(context.Background(), pty.AttachOptions{Replay: pty.ReplayHistory}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if result.ReplayFirst != 1 || result.ReplayLast != 1 || result.NextLive != 2 || result.Truncated {
		t.Fatalf("AttachResult = %#v", result)
	}
	assertOutputEvent(t, sink.events, 1, "history")
	go factory.process(0).write("live")
	assertOutputEvent(t, sink.events, 2, "live")
	if err = attachment.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-factory.process(0).done:
		t.Fatal("detach stopped process")
	default:
	}
	resume := &collectingOutput{events: make(chan pty.OutputEvent, 4)}
	resumed, resumeResult, err := session.Attach(context.Background(), pty.AttachOptions{Replay: pty.ReplayHistory, ResumeAfter: 1}, resume)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	if resumeResult.ReplayFirst != 2 || resumeResult.ReplayLast != 2 {
		t.Fatalf("resume result = %#v", resumeResult)
	}
	assertOutputEvent(t, resume.events, 2, "live")
	factory.mu.Lock()
	gotSpec := factory.specs[0]
	factory.mu.Unlock()
	if !reflect.DeepEqual(gotSpec.Args, []string{"one"}) || !reflect.DeepEqual(gotSpec.Env, []string{"A=B"}) {
		t.Fatalf("ProcessSpec was aliased: %#v", gotSpec)
	}
	factory.process(0).finish(pty.ExitStatus{Reason: pty.ExitReasonExited, Code: 7})
	status, err := session.Wait(context.Background())
	if err != nil || status.Code != 7 {
		t.Fatalf("Wait = %#v, %v", status, err)
	}
}

func TestSlowAttachmentOverflowDoesNotStopSession(t *testing.T) {
	factory := &managedTestFactory{}
	config := pty.DefaultManagerConfig()
	config.HistoryBytes = 0
	config.MaxTotalHistoryBytes = 0
	config.AttachmentQueueBytes = 4
	manager, _ := pty.NewManager(context.Background(), factory, config)
	defer manager.Close()
	session, _ := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	sink := &blockingOutput{started: make(chan struct{})}
	attachment, _, err := session.Attach(context.Background(), pty.AttachOptions{}, sink)
	if err != nil {
		t.Fatal(err)
	}
	go factory.process(0).write("aa")
	select {
	case <-sink.started:
	case <-time.After(time.Second):
		t.Fatal("slow sink did not start")
	}
	for _, data := range []string{"bb", "cc", "dd"} {
		go factory.process(0).write(data)
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case <-attachment.Done():
	case <-time.After(time.Second):
		t.Fatal("overflow did not detach slow sink")
	}
	if attachment.Err() == nil {
		t.Fatal("overflow error was not retained")
	}
	select {
	case <-factory.process(0).done:
		t.Fatal("overflow stopped PTY Session")
	default:
	}
}

func TestReplayLargerThanAttachmentQueueStreamsBeforeLiveOutput(t *testing.T) {
	factory := &managedTestFactory{}
	config := pty.DefaultManagerConfig()
	config.HistoryBytes = 16
	config.MaxTotalHistoryBytes = 16
	config.AttachmentQueueBytes = 4
	manager, err := pty.NewManager(context.Background(), factory, config)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	session, err := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	if err != nil {
		t.Fatal(err)
	}

	for sequence, data := range []string{"aaaa", "bbbb", "cccc"} {
		go factory.process(0).write(data)
		waitSessionSequence(t, session, uint64(sequence+1))
	}
	sink := &collectingOutput{events: make(chan pty.OutputEvent, 4)}
	attachment, result, err := session.Attach(context.Background(), pty.AttachOptions{Replay: pty.ReplayHistory, Paused: true}, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer attachment.Close()
	if result.ReplayFirst != 1 || result.ReplayLast != 3 || result.NextLive != 4 || result.Truncated {
		t.Fatalf("AttachResult = %#v", result)
	}

	go factory.process(0).write("live")
	waitSessionSequence(t, session, 4)
	if err = attachment.Activate(); err != nil {
		t.Fatal(err)
	}
	for sequence, data := range []string{"aaaa", "bbbb", "cccc", "live"} {
		assertOutputEvent(t, sink.events, uint64(sequence+1), data)
	}
}

func TestManagerCloseTerminatesAndReapsAllProcesses(t *testing.T) {
	factory := &managedTestFactory{}
	config := pty.DefaultManagerConfig()
	config.HistoryBytes = 0
	config.MaxTotalHistoryBytes = 0
	manager, _ := pty.NewManager(context.Background(), factory, config)
	for range 3 {
		if _, err := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	for index := range 3 {
		process := factory.process(index)
		select {
		case <-process.done:
		default:
			t.Fatalf("process %d not reaped", index)
		}
		process.mu.Lock()
		closed := process.closed
		process.mu.Unlock()
		if !closed {
			t.Fatalf("process %d handle not closed", index)
		}
	}
}

func TestManagerCloseCancelsAndJoinsStartup(t *testing.T) {
	factory := &blockingManagedFactory{started: make(chan struct{})}
	config := pty.DefaultManagerConfig()
	config.MaxSessions = 1
	manager, err := pty.NewManager(context.Background(), factory, config)
	if err != nil {
		t.Fatal(err)
	}
	openDone := make(chan error, 1)
	go func() {
		_, openErr := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
		openDone <- openErr
	}()
	<-factory.started
	if _, err = manager.Open(context.Background(), pty.ProcessSpec{Command: "second", InitialSize: pty.Size{Cols: 80, Rows: 24}}); !errors.Is(err, pty.ErrSessionLimit) {
		t.Fatalf("second Open = %v, want session limit", err)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-openDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Open after Manager.Close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Manager.Close did not join startup")
	}
}

func TestManagerObserverOverflowIsIsolated(t *testing.T) {
	factory := &managedTestFactory{}
	config := pty.DefaultManagerConfig()
	config.HistoryBytes = 0
	config.MaxTotalHistoryBytes = 0
	config.ObserverQueueBytes = 192
	manager, _ := pty.NewManager(context.Background(), factory, config)
	defer manager.Close()
	subscription, err := manager.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	// Do not consume Events: the first event blocks only this observer worker.
	_, err = manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 8; index++ {
		go factory.process(0).write("observer-output")
		time.Sleep(time.Millisecond)
	}
	select {
	case <-subscription.Done():
	case <-time.After(time.Second):
		t.Fatal("observer overflow did not close subscription")
	}
	if subscription.Err() == nil {
		t.Fatal("observer overflow error missing")
	}
	select {
	case <-factory.process(0).done:
		t.Fatal("observer overflow stopped PTY")
	default:
	}
}

func TestHistoryRetainsTailOfOversizedChunk(t *testing.T) {
	factory := &managedTestFactory{}
	config := pty.DefaultManagerConfig()
	config.HistoryBytes = 4
	config.MaxTotalHistoryBytes = 4
	manager, _ := pty.NewManager(context.Background(), factory, config)
	defer manager.Close()
	session, _ := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	go factory.process(0).write("0123456789")
	waitSessionSequence(t, session, 1)
	sink := &collectingOutput{events: make(chan pty.OutputEvent, 1)}
	attachment, _, err := session.Attach(context.Background(), pty.AttachOptions{Replay: pty.ReplayHistory}, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer attachment.Close()
	assertOutputEvent(t, sink.events, 1, "6789")
}

func TestManagerOperatesMoreThanTwentySessionsIndependently(t *testing.T) {
	factory := &managedTestFactory{}
	config := pty.DefaultManagerConfig()
	config.HistoryBytes = 0
	config.MaxTotalHistoryBytes = 0
	manager, err := pty.NewManager(context.Background(), factory, config)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	const count = 24
	for index := 0; index < count; index++ {
		session, openErr := manager.Open(context.Background(), pty.ProcessSpec{Command: "shell", InitialSize: pty.Size{Cols: 80, Rows: 24}})
		if openErr != nil {
			t.Fatalf("Open %d: %v", index, openErr)
		}
		sink := &collectingOutput{events: make(chan pty.OutputEvent, 1)}
		if _, _, openErr = session.Attach(context.Background(), pty.AttachOptions{}, sink); openErr != nil {
			t.Fatalf("Attach %d: %v", index, openErr)
		}
		if openErr = session.Input(context.Background(), []byte{byte(index)}); openErr != nil {
			t.Fatalf("Input %d: %v", index, openErr)
		}
		wantSize := pty.Size{Cols: 100 + index, Rows: 30}
		if openErr = session.Resize(context.Background(), wantSize); openErr != nil {
			t.Fatalf("Resize %d: %v", index, openErr)
		}
		go factory.process(index).write(string([]byte{byte(index)}))
		assertOutputEvent(t, sink.events, 1, string([]byte{byte(index)}))
		if session.Info().Size != wantSize {
			t.Fatalf("Session %d size = %#v", index, session.Info().Size)
		}
	}
	stats := manager.Stats()
	if stats.Sessions != count || stats.Attachments != count {
		t.Fatalf("Manager stats = %#v", stats)
	}
}

func TestManagerGlobalHistoryEvictsOldestChunkAcrossSessions(t *testing.T) {
	factory := &managedTestFactory{}
	config := pty.DefaultManagerConfig()
	config.HistoryBytes = 6
	config.MaxTotalHistoryBytes = 6
	config.MaxAttachmentsPerSession = 2
	manager, err := pty.NewManager(context.Background(), factory, config)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	one, _ := manager.Open(context.Background(), pty.ProcessSpec{Command: "one", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	two, _ := manager.Open(context.Background(), pty.ProcessSpec{Command: "two", InitialSize: pty.Size{Cols: 80, Rows: 24}})
	go factory.process(0).write("aaa")
	waitSessionSequence(t, one, 1)
	go factory.process(1).write("bbb")
	waitSessionSequence(t, two, 1)
	go factory.process(0).write("ccc")
	waitSessionSequence(t, one, 2)

	oneSink := &collectingOutput{events: make(chan pty.OutputEvent, 2)}
	twoSink := &collectingOutput{events: make(chan pty.OutputEvent, 1)}
	if _, _, err = one.Attach(context.Background(), pty.AttachOptions{Replay: pty.ReplayHistory}, oneSink); err != nil {
		t.Fatal(err)
	}
	if _, _, err = two.Attach(context.Background(), pty.AttachOptions{Replay: pty.ReplayHistory}, twoSink); err != nil {
		t.Fatal(err)
	}
	assertOutputEvent(t, oneSink.events, 2, "ccc")
	assertOutputEvent(t, twoSink.events, 1, "bbb")
	if stats := manager.Stats(); stats.HistoryBytes != 6 || stats.HistoryTruncations == 0 {
		t.Fatalf("Manager history stats = %#v", stats)
	}
}

func waitSessionSequence(t *testing.T, session *pty.Session, want uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if session.Info().LastSequence >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("sequence did not reach %d", want)
}
func assertOutputEvent(t *testing.T, events <-chan pty.OutputEvent, sequence uint64, data string) {
	t.Helper()
	select {
	case event := <-events:
		if event.Sequence != sequence || string(event.Data) != data {
			t.Fatalf("event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("output event timed out")
	}
}
