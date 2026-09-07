package pty

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aruzen/streammux"
)

var (
	ErrNilManagedFactory = errors.New("pty: nil managed factory")
	ErrManagerClosed     = errors.New("pty: manager closed")
	ErrSessionLimit      = errors.New("pty: session limit reached")
	ErrAttachmentLimit   = errors.New("pty: attachment limit reached")
	ErrObserverLimit     = errors.New("pty: observer limit reached")
	ErrQueueOverflow     = errors.New("pty: queue byte limit exceeded")
	ErrAlreadyAttached   = errors.New("pty: output sink already attached")
	ErrFutureSequence    = errors.New("pty: resume sequence is in the future")
	ErrAttachmentClosed  = errors.New("pty: attachment closed")
	ErrObserverClosed    = errors.New("pty: observer closed")
	ErrSessionRunning    = errors.New("pty: session is still running")
	ErrSessionExited     = errors.New("pty: session has exited")
)

// ManagerConfig sets hard resource limits for a Manager. All byte limits count
// terminal output bytes only. Use DefaultManagerConfig and override fields.
type ManagerConfig struct {
	// MaxSessions bounds processes that are starting, running, or stopping.
	// Retained exited Sessions use the separate lifecycle descriptor limit.
	MaxSessions          int
	HistoryBytes         int
	MaxTotalHistoryBytes int64
	ReadBufferBytes      int
	AttachmentQueueBytes int
	ObserverQueueBytes   int
	MaxObservers         int
	// SessionLifecycle selects whether exited Sessions are removed or retained.
	SessionLifecycle SessionLifecycleDescriptor
	// MaxAttachmentsPerSession is a hard per-Session limit.
	MaxAttachmentsPerSession int
	// AttachmentDrainTimeout bounds terminal OutputSink delivery after process
	// exit. Zero detaches immediately.
	AttachmentDrainTimeout time.Duration
	GracefulKillTimeout    time.Duration
}

// ExitedSessionPolicy selects the registry lifetime after process exit.
type ExitedSessionPolicy uint8

const (
	// ExitedSessionRemove removes the Session after terminal attachment events
	// drain. This is the zero value and the default policy.
	ExitedSessionRemove ExitedSessionPolicy = iota
	// ExitedSessionRetain keeps bounded history and exit status for reattach.
	ExitedSessionRetain
)

// SessionLifecycleDescriptor fixes the exited-Session policy when a Manager
// starts. Retention must be explicitly enabled with a positive bound.
type SessionLifecycleDescriptor struct {
	// ExitPolicy selects immediate removal or bounded retention.
	ExitPolicy ExitedSessionPolicy
	// MaxRetainedSessions is zero for removal and positive for retention.
	MaxRetainedSessions int
}

// DefaultManagerConfig returns bounded defaults suitable for a daemon.
func DefaultManagerConfig() ManagerConfig {
	return ManagerConfig{
		MaxSessions: 256, HistoryBytes: 16 << 20, MaxTotalHistoryBytes: 512 << 20,
		ReadBufferBytes: DefaultReadBufferSize, AttachmentQueueBytes: 4 << 20,
		ObserverQueueBytes: 4 << 20, MaxObservers: 32,
		SessionLifecycle:         SessionLifecycleDescriptor{ExitPolicy: ExitedSessionRemove},
		MaxAttachmentsPerSession: 1,
		AttachmentDrainTimeout:   2 * time.Second, GracefulKillTimeout: 2 * time.Second,
	}
}

func (c ManagerConfig) validate() error {
	if c.MaxSessions <= 0 || c.HistoryBytes < 0 || c.MaxTotalHistoryBytes < 0 ||
		c.ReadBufferBytes <= 0 || c.AttachmentQueueBytes <= 0 || c.ObserverQueueBytes <= 0 ||
		c.MaxObservers <= 0 || c.MaxAttachmentsPerSession <= 0 ||
		c.AttachmentDrainTimeout < 0 || c.GracefulKillTimeout < 0 {
		return errors.New("pty: invalid ManagerConfig")
	}
	switch c.SessionLifecycle.ExitPolicy {
	case ExitedSessionRemove:
		if c.SessionLifecycle.MaxRetainedSessions != 0 {
			return errors.New("pty: removal policy cannot retain Sessions")
		}
	case ExitedSessionRetain:
		if c.SessionLifecycle.MaxRetainedSessions <= 0 {
			return errors.New("pty: retention policy requires a positive Session limit")
		}
	default:
		return errors.New("pty: invalid exited Session policy")
	}
	if c.HistoryBytes > 0 && int64(c.HistoryBytes) > c.MaxTotalHistoryBytes && c.MaxTotalHistoryBytes != 0 {
		return errors.New("pty: per-session history exceeds total history")
	}
	return nil
}

// SessionState is the externally observable managed-process lifecycle state.
type SessionState string

const (
	SessionStarting SessionState = "starting"
	SessionRunning  SessionState = "running"
	SessionStopping SessionState = "stopping"
	SessionExited   SessionState = "exited"
	SessionFailed   SessionState = "failed"
)

// SessionInfo is an immutable point-in-time Session snapshot. HistoryAvailable
// reports whether at least one output chunk is currently replayable.
type SessionInfo struct {
	ID               streammux.StreamID `json:"id"`
	State            SessionState       `json:"state"`
	Size             Size               `json:"size"`
	StartedAt        time.Time          `json:"started_at"`
	LastSequence     uint64             `json:"last_sequence"`
	Exit             *ExitStatus        `json:"exit,omitempty"`
	AttachmentCount  int                `json:"attachment_count"`
	HistoryAvailable bool               `json:"history_available"`
}

// ReplayMode selects which retained output precedes live attachment output.
type ReplayMode string

const (
	ReplayNone    ReplayMode = "none"
	ReplayHistory ReplayMode = "history"
)

// AttachOptions controls replay and activation for a new attachment.
type AttachOptions struct {
	Replay      ReplayMode
	ResumeAfter uint64
	// Paused queues replay/live output but does not invoke OutputSink until
	// Activate is called. Protocol adapters use this to send AttachResponse
	// before replay.
	Paused bool
}

// OutputEvent contains one ordered output chunk. Data may be retained by a
// consumer but is shared and must never be mutated.
type OutputEvent struct {
	Sequence uint64
	Data     []byte
}

// AttachmentEventKind identifies an item in an attachment's ordered queue.
type AttachmentEventKind string

const (
	AttachmentOutput AttachmentEventKind = "output"
	AttachmentExit   AttachmentEventKind = "exit"
)

// AttachmentEvent is either immutable output or the terminal exit status.
// An exit event is delivered after all preceding output and is the final item.
type AttachmentEvent struct {
	Kind   AttachmentEventKind
	Output *OutputEvent
	Exit   *ExitStatus
}

// AttachResult describes the replay interval atomically captured by Attach.
type AttachResult struct {
	ReplayFirst uint64 `json:"replay_first"`
	ReplayLast  uint64 `json:"replay_last"`
	NextLive    uint64 `json:"next_live"`
	Truncated   bool   `json:"truncated"`
}

// OutputSink consumes one attachment serially. Send must honor ctx. Event
// fields and output bytes may be retained but are immutable.
type OutputSink interface {
	Send(context.Context, AttachmentEvent) error
}

// AttachmentCloseReason records why an attachment stopped.
type AttachmentCloseReason string

const (
	AttachmentClosedExplicitly AttachmentCloseReason = "explicit"
	AttachmentContextCanceled  AttachmentCloseReason = "context_canceled"
	AttachmentSessionExited    AttachmentCloseReason = "session_exited"
	AttachmentQueueOverflow    AttachmentCloseReason = "queue_overflow"
	AttachmentSinkFailure      AttachmentCloseReason = "sink_failure"
	AttachmentDrainTimeout     AttachmentCloseReason = "drain_timeout"
	AttachmentManagerClosed    AttachmentCloseReason = "manager_closed"
	AttachmentSessionRemoved   AttachmentCloseReason = "session_removed"
	AttachmentSessionEvicted   AttachmentCloseReason = "session_evicted"
)

// ObserverCloseReason records why an observer subscription stopped.
type ObserverCloseReason string

const (
	ObserverClosedExplicitly ObserverCloseReason = "explicit"
	ObserverContextCanceled  ObserverCloseReason = "context_canceled"
	ObserverQueueOverflow    ObserverCloseReason = "queue_overflow"
	ObserverManagerClosed    ObserverCloseReason = "manager_closed"
)

// EventKind identifies an observer event.
type EventKind string

const (
	EventOpened   EventKind = "opened"
	EventOutput   EventKind = "output"
	EventExited   EventKind = "exited"
	EventAttached EventKind = "attached"
	EventDetached EventKind = "detached"
	EventRetained EventKind = "retained"
	EventEvicted  EventKind = "evicted"
	EventRemoved  EventKind = "removed"
)

// AttachmentInfo identifies one attachment involved in a Manager event.
type AttachmentInfo struct {
	ID     uint64                `json:"id"`
	Reason AttachmentCloseReason `json:"reason,omitempty"`
}

// Event is an immutable Manager observer event. Output data is shared and must
// not be mutated.
type Event struct {
	Kind       EventKind
	Session    SessionInfo
	Output     *OutputEvent
	Attachment *AttachmentInfo
}

type historyChunk struct {
	OutputEvent
	order uint64
}

type retainedRemoval struct {
	session          *Session
	attachments      []*Attachment
	attachmentReason AttachmentCloseReason
	eventKind        EventKind
}

// Manager owns managed PTY processes independently of any transport
// connection. Its methods are safe for concurrent use.
type Manager struct {
	ctx                   context.Context
	cancel                context.CancelFunc
	factory               ManagedFactory
	config                ManagerConfig
	mu                    sync.Mutex
	sessions              map[streammux.StreamID]*Session
	retainedExited        []streammux.StreamID
	observers             map[uint64]*Subscription
	nextSession           atomic.Uint64
	nextAttachment        atomic.Uint64
	nextObserver          atomic.Uint64
	nextHistory           uint64
	historyBytes          int64
	historyTruncations    uint64
	historyTruncatedBytes uint64
	processesStarted      uint64
	processesExited       uint64
	sessionsEvicted       uint64
	sessionsRemoved       uint64
	attachmentClosures    map[AttachmentCloseReason]uint64
	observerClosures      map[ObserverCloseReason]uint64
	generation            uint64
	sessionIDExhausted    bool
	closing               bool
	closeOnce             sync.Once
	closeErr              error
	closed                chan struct{}
	workers               sync.WaitGroup
	starts                sync.WaitGroup
	starting              int
	activeSessions        int
}

// ManagerStats is a point-in-time resource and cumulative lifecycle snapshot.
// Returned maps are copies and PTY payloads are never included.
type ManagerStats struct {
	// Sessions is the total registered count, including retained exited
	// Sessions. ActiveSessions excludes retained and removal-in-progress entries.
	Sessions               int
	ActiveSessions         int
	StartingSessions       int
	Attachments            int
	Subscriptions          int
	HistoryBytes           int64
	HistoryTruncations     uint64
	HistoryTruncatedBytes  uint64
	ProcessesStarted       uint64
	ProcessesExited        uint64
	RetainedExitedSessions int
	SessionsEvicted        uint64
	SessionsRemoved        uint64
	AttachmentClosures     map[AttachmentCloseReason]uint64
	ObserverClosures       map[ObserverCloseReason]uint64
}

// NewManager takes ownership of every process successfully started through
// factory. Canceling parent initiates Close. The caller must call Close and may
// do so concurrently with any other operation.
func NewManager(parent context.Context, factory ManagedFactory, config ManagerConfig) (*Manager, error) {
	if parent == nil {
		return nil, errors.New("pty: nil Manager context")
	}
	if factory == nil {
		return nil, ErrNilManagedFactory
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	m := &Manager{ctx: ctx, cancel: cancel, factory: factory, config: config, sessions: make(map[streammux.StreamID]*Session), observers: make(map[uint64]*Subscription), attachmentClosures: make(map[AttachmentCloseReason]uint64), observerClosures: make(map[ObserverCloseReason]uint64), closed: make(chan struct{})}
	go func() { <-ctx.Done(); _ = m.Close() }()
	return m, nil
}

// Open starts and registers one Session. ctx controls startup only; once Open
// returns a Session, Manager owns its lifetime. Args and Env are copied before
// the backend is called.
func (m *Manager) Open(ctx context.Context, spec ProcessSpec) (*Session, error) {
	if ctx == nil {
		return nil, errors.New("pty: nil Open context")
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	if err := spec.InitialSize.Validate(); err != nil {
		return nil, err
	}
	spec.Args = append([]string(nil), spec.Args...)
	if spec.Env != nil {
		spec.Env = append([]string(nil), spec.Env...)
	}
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return nil, ErrManagerClosed
	}
	if m.activeSessions+m.starting >= m.config.MaxSessions {
		m.mu.Unlock()
		return nil, ErrSessionLimit
	}
	if m.sessionIDExhausted {
		m.mu.Unlock()
		return nil, ErrSessionLimit
	}
	id := streammux.StreamID(m.nextSession.Add(1))
	if id == 0 {
		m.sessionIDExhausted = true
		m.mu.Unlock()
		return nil, ErrSessionLimit
	}
	m.starting++
	m.starts.Add(1)
	m.mu.Unlock()
	defer m.starts.Done()
	startCtx, cancelStart := context.WithCancel(ctx)
	stopManagerCancel := context.AfterFunc(m.ctx, cancelStart)
	process, err := m.factory.StartManaged(startCtx, spec)
	stopManagerCancel()
	cancelStart()
	if err != nil {
		m.mu.Lock()
		m.starting--
		m.mu.Unlock()
		return nil, err
	}
	m.mu.Lock()
	m.processesStarted++
	m.mu.Unlock()
	if ctx.Err() != nil {
		m.mu.Lock()
		m.starting--
		m.mu.Unlock()
		_ = process.Kill()
		_, _ = process.WaitStatus()
		_ = process.Close()
		m.recordProcessExited()
		return nil, ctx.Err()
	}
	s := &Session{manager: m, id: id, process: process, done: make(chan struct{}), state: SessionRunning, size: spec.InitialSize, startedAt: time.Now(), attachments: make(map[uint64]*Attachment)}
	m.mu.Lock()
	m.starting--
	if m.closing || m.activeSessions >= m.config.MaxSessions {
		m.mu.Unlock()
		_ = process.Kill()
		_, _ = process.WaitStatus()
		_ = process.Close()
		m.recordProcessExited()
		if m.closing {
			return nil, ErrManagerClosed
		}
		return nil, ErrSessionLimit
	}
	m.sessions[id] = s
	m.activeSessions++
	m.generation++
	failedObservers := m.emitLocked(Event{Kind: EventOpened, Session: s.infoLocked()})
	m.mu.Unlock()
	finishObservers(failedObservers, ErrQueueOverflow, ObserverQueueOverflow)
	go s.run()
	return s, nil
}

// Get returns a concurrently usable Session while it remains registered.
func (m *Manager) Get(id streammux.StreamID) (*Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	return s, ok
}

// List returns sorted snapshots of all currently registered sessions.
func (m *Manager) List() []SessionInfo {
	_, result := m.ListSnapshot()
	return result
}

// ListSnapshot returns a generation and sorted Session snapshots suitable for
// stable protocol pagination.
func (m *Manager) ListSnapshot() (uint64, []SessionInfo) {
	m.mu.Lock()
	result := make([]SessionInfo, 0, len(m.sessions))
	for _, s := range m.sessions {
		result = append(result, s.infoLocked())
	}
	generation := m.generation
	m.mu.Unlock()
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return generation, result
}

// SnapshotGeneration returns the current session-registry generation.
func (m *Manager) SnapshotGeneration() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.generation
}

// Stats reports current resource usage without sharing internal storage.
func (m *Manager) Stats() ManagerStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := ManagerStats{Sessions: len(m.sessions), ActiveSessions: m.activeSessions, StartingSessions: m.starting, Subscriptions: len(m.observers), HistoryBytes: m.historyBytes, HistoryTruncations: m.historyTruncations, HistoryTruncatedBytes: m.historyTruncatedBytes, ProcessesStarted: m.processesStarted, ProcessesExited: m.processesExited, RetainedExitedSessions: len(m.retainedExited), SessionsEvicted: m.sessionsEvicted, SessionsRemoved: m.sessionsRemoved, AttachmentClosures: make(map[AttachmentCloseReason]uint64, len(m.attachmentClosures)), ObserverClosures: make(map[ObserverCloseReason]uint64, len(m.observerClosures))}
	for _, session := range m.sessions {
		stats.Attachments += len(session.attachments)
	}
	for reason, count := range m.attachmentClosures {
		stats.AttachmentClosures[reason] = count
	}
	for reason, count := range m.observerClosures {
		stats.ObserverClosures[reason] = count
	}
	return stats
}

// Kill forcefully ends a Session and waits for its output, attachment exit
// events, and process resources to be drained. ctx only bounds the wait.
func (m *Manager) Kill(ctx context.Context, id streammux.StreamID) error {
	s, ok := m.Get(id)
	if !ok {
		return ErrSessionNotFound
	}
	return s.Kill(ctx)
}

// Remove deletes one retained exited Session and its history. Running Sessions
// must be stopped first. Any attachment replaying the retained Session is
// detached and its worker has exited before Remove returns.
func (m *Manager) Remove(id streammux.StreamID) error {
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return ErrManagerClosed
	}
	s, ok := m.sessions[id]
	if !ok {
		m.mu.Unlock()
		return ErrSessionNotFound
	}
	if s.state == SessionRunning || s.state == SessionStopping {
		m.mu.Unlock()
		return ErrSessionRunning
	}
	removal, ok := m.beginRetainedRemovalLocked(s, AttachmentSessionRemoved, EventRemoved)
	if !ok {
		m.mu.Unlock()
		return ErrSessionNotFound
	}
	m.mu.Unlock()
	m.finishRetainedRemoval(removal)
	return nil
}

// Subscribe creates an independently bounded Manager event observer.
func (m *Manager) Subscribe() (*Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing {
		return nil, ErrManagerClosed
	}
	if len(m.observers) >= m.config.MaxObservers {
		return nil, ErrObserverLimit
	}
	id := m.nextObserver.Add(1)
	s := newSubscription(m, id, m.config.ObserverQueueBytes)
	m.observers[id] = s
	m.workers.Add(1)
	go s.run()
	return s, nil
}

// Close is idempotent. It rejects new opens, cancels startups, terminates then
// force-kills remaining sessions, reaps and closes every process, closes all
// observers, and joins all Manager workers.
func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closing = true
		sessions := make([]*Session, 0, len(m.sessions))
		active := make([]*Session, 0, len(m.sessions))
		for _, s := range m.sessions {
			sessions = append(sessions, s)
			if s.state == SessionRunning || s.state == SessionStopping {
				active = append(active, s)
			}
			if s.state == SessionRunning {
				s.state = SessionStopping
			}
		}
		observers := make([]*Subscription, 0, len(m.observers))
		for _, s := range m.observers {
			observers = append(observers, s)
		}
		clear(m.observers)
		m.mu.Unlock()
		m.cancel()
		m.starts.Wait()
		finishObservers(observers, nil, ObserverManagerClosed)
		m.closeErr = errors.Join(m.closeErr, runSessionOperation(active, func(s *Session) error {
			return s.process.Terminate()
		}))
		deadline := time.NewTimer(m.config.GracefulKillTimeout)
		defer deadline.Stop()
		remaining := append([]*Session(nil), active...)
		if m.config.GracefulKillTimeout > 0 {
			select {
			case <-allSessionsDone(remaining):
				remaining = nil
			case <-deadline.C:
			}
		} else if !deadline.Stop() {
			<-deadline.C
		}
		m.closeErr = errors.Join(m.closeErr, runSessionOperation(remaining, func(s *Session) error {
			select {
			case <-s.done:
				return nil
			default:
				return s.process.Kill()
			}
		}))
		for _, s := range sessions {
			<-s.done
		}
		m.workers.Wait()
		m.mu.Lock()
		for _, s := range m.sessions {
			m.removeHistoryLocked(s)
			s.retained = false
			s.removing = false
		}
		clear(m.sessions)
		m.retainedExited = nil
		m.activeSessions = 0
		m.generation++
		m.mu.Unlock()
		close(m.closed)
	})
	<-m.closed
	return m.closeErr
}

func runSessionOperation(sessions []*Session, operation func(*Session) error) error {
	errorsBySession := make(chan error, len(sessions))
	var wait sync.WaitGroup
	for _, session := range sessions {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := operation(session); err != nil {
				errorsBySession <- err
			}
		}()
	}
	wait.Wait()
	close(errorsBySession)
	var result error
	for err := range errorsBySession {
		result = errors.Join(result, err)
	}
	return result
}

func allSessionsDone(sessions []*Session) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		for _, s := range sessions {
			<-s.done
		}
		close(done)
	}()
	return done
}

// Session is one Manager-owned PTY process. It remains usable across transport
// disconnects until process exit or Manager shutdown. An exited Session remains
// attachable only when its Manager explicitly enables retention.
type Session struct {
	manager      *Manager
	id           streammux.StreamID
	process      ManagedProcess
	inputMu      sync.Mutex
	done         chan struct{}
	state        SessionState
	size         Size
	startedAt    time.Time
	lastSequence uint64
	exit         *ExitStatus
	retained     bool
	removing     bool
	history      []historyChunk
	historyBytes int
	attachments  map[uint64]*Attachment
	killOnce     sync.Once
	killErr      error
}

// ID returns the non-zero Manager-allocated stream identifier.
func (s *Session) ID() streammux.StreamID { return s.id }

// Info returns an immutable lifecycle snapshot.
func (s *Session) Info() SessionInfo {
	s.manager.mu.Lock()
	defer s.manager.mu.Unlock()
	return s.infoLocked()
}
func (s *Session) infoLocked() SessionInfo {
	info := SessionInfo{ID: s.id, State: s.state, Size: s.size, StartedAt: s.startedAt, LastSequence: s.lastSequence, AttachmentCount: len(s.attachments), HistoryAvailable: len(s.history) > 0}
	if s.exit != nil {
		value := *s.exit
		info.Exit = &value
	}
	return info
}

// Input writes all data serially to the PTY. The caller retains ownership and
// may reuse data after return. ctx is checked between underlying writes.
func (s *Session) Input(ctx context.Context, data []byte) error {
	if ctx == nil {
		return errors.New("pty: nil Input context")
	}
	s.inputMu.Lock()
	defer s.inputMu.Unlock()
	s.manager.mu.Lock()
	running := s.state == SessionRunning
	s.manager.mu.Unlock()
	if !running {
		return ErrSessionExited
	}
	return writeFullContext(ctx, s.process.Input(), data)
}

func writeFullContext(ctx context.Context, writer io.Writer, data []byte) error {
	for len(data) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, err := writer.Write(data)
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// Resize validates and applies a terminal size unless ctx is already done.
func (s *Session) Resize(ctx context.Context, size Size) error {
	if ctx == nil {
		return errors.New("pty: nil Resize context")
	}
	if err := size.Validate(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.manager.mu.Lock()
	running := s.state == SessionRunning
	s.manager.mu.Unlock()
	if !running {
		return ErrSessionExited
	}
	if err := s.process.Resize(size); err != nil {
		return err
	}
	s.manager.mu.Lock()
	if s.state == SessionRunning {
		s.size = size
	}
	s.manager.mu.Unlock()
	return nil
}

// Attach creates a bounded queue that serializes replay, live output, and one
// terminal exit event into sink. ctx owns the attachment lifetime but never
// the Session lifetime. On success the caller should eventually call Close.
func (s *Session) Attach(ctx context.Context, options AttachOptions, sink OutputSink) (*Attachment, AttachResult, error) {
	if ctx == nil {
		return nil, AttachResult{}, errors.New("pty: nil Attach context")
	}
	select {
	case <-ctx.Done():
		return nil, AttachResult{}, ctx.Err()
	default:
	}
	if sink == nil {
		return nil, AttachResult{}, errors.New("pty: nil OutputSink")
	}
	if options.Replay == "" {
		options.Replay = ReplayNone
	}
	if options.Replay != ReplayNone && options.Replay != ReplayHistory {
		return nil, AttachResult{}, errors.New("pty: invalid replay mode")
	}
	m := s.manager
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return nil, AttachResult{}, ErrSessionNotFound
	}
	if s.state != SessionRunning && !(s.state == SessionExited && s.retained) {
		m.mu.Unlock()
		return nil, AttachResult{}, ErrSessionExited
	}
	if len(s.attachments) >= m.config.MaxAttachmentsPerSession {
		m.mu.Unlock()
		return nil, AttachResult{}, ErrAttachmentLimit
	}
	if options.ResumeAfter > s.lastSequence {
		m.mu.Unlock()
		return nil, AttachResult{}, ErrFutureSequence
	}
	id := m.nextAttachment.Add(1)
	if id == 0 {
		m.mu.Unlock()
		return nil, AttachResult{}, ErrAttachmentLimit
	}
	a := newAttachment(ctx, s, id, sink, m.config.AttachmentQueueBytes, !options.Paused)
	result := AttachResult{NextLive: s.lastSequence + 1}
	if options.Replay == ReplayHistory {
		if len(s.history) > 0 && options.ResumeAfter+1 < s.history[0].Sequence {
			result.Truncated = true
		}
		for _, chunk := range s.history {
			if chunk.Sequence <= options.ResumeAfter {
				continue
			}
			if result.ReplayFirst == 0 {
				result.ReplayFirst = chunk.Sequence
			}
			result.ReplayLast = chunk.Sequence
			replay := chunk.OutputEvent
			if err := a.enqueueLocked(AttachmentEvent{Kind: AttachmentOutput, Output: &replay}); err != nil {
				m.mu.Unlock()
				a.finish(err, AttachmentQueueOverflow)
				return nil, AttachResult{}, err
			}
		}
	}
	if s.state == SessionExited && s.exit != nil {
		if err := a.enqueueExitLocked(*s.exit); err != nil {
			m.mu.Unlock()
			a.finish(err, AttachmentQueueOverflow)
			return nil, AttachResult{}, err
		}
	}
	if !a.start() {
		m.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, AttachResult{}, err
		}
		return nil, AttachResult{}, ErrAttachmentClosed
	}
	s.attachments[id] = a
	a.registered = true
	m.workers.Add(1)
	failedObservers := m.emitLocked(Event{Kind: EventAttached, Session: s.infoLocked(), Attachment: &AttachmentInfo{ID: id}})
	m.mu.Unlock()
	finishObservers(failedObservers, ErrQueueOverflow, ObserverQueueOverflow)
	go a.run()
	return a, result, nil
}

// Kill signals the process once and waits for complete Session cleanup. A
// canceled ctx stops only the caller's wait; cleanup continues.
func (s *Session) Kill(ctx context.Context) error {
	if ctx == nil {
		return errors.New("pty: nil Kill context")
	}
	s.killOnce.Do(func() {
		s.manager.mu.Lock()
		shouldKill := s.state == SessionRunning || s.state == SessionStopping
		if s.state == SessionRunning {
			s.state = SessionStopping
		}
		s.manager.mu.Unlock()
		if shouldKill {
			s.killErr = s.process.Kill()
		}
	})
	_, waitErr := s.Wait(ctx)
	return errors.Join(s.killErr, waitErr)
}

// Wait returns the immutable exit status after output and all terminal
// attachment events present at process exit have drained. A retained exited
// Session may remain registered until Remove or retention eviction.
func (s *Session) Wait(ctx context.Context) (ExitStatus, error) {
	if ctx == nil {
		return ExitStatus{}, errors.New("pty: nil Wait context")
	}
	select {
	case <-s.done:
		s.manager.mu.Lock()
		defer s.manager.mu.Unlock()
		if s.exit == nil {
			return ExitStatus{}, ErrSessionNotFound
		}
		return *s.exit, nil
	case <-ctx.Done():
		return ExitStatus{}, ctx.Err()
	}
}

func (s *Session) run() {
	outputDone := make(chan error, 1)
	go func() { outputDone <- s.pumpOutput() }()
	waitDone := make(chan struct {
		status ExitStatus
		err    error
	}, 1)
	go func() {
		status, err := s.process.WaitStatus()
		waitDone <- struct {
			status ExitStatus
			err    error
		}{status, err}
	}()
	var status ExitStatus
	var waitErr, outputErr error
	select {
	case result := <-waitDone:
		status, waitErr = result.status, result.err
		outputErr = <-outputDone
	case outputErr = <-outputDone:
		if outputErr != nil {
			_ = s.process.Kill()
		}
		result := <-waitDone
		status, waitErr = result.status, result.err
	}
	if waitErr != nil && status.Error == "" {
		status.Error = waitErr.Error()
	}
	if outputErr != nil && status.Error == "" {
		status.Reason = ExitReasonIOFailure
		status.Error = outputErr.Error()
	}
	_ = s.process.Close()
	m := s.manager
	m.mu.Lock()
	m.processesExited++
	s.exit = &status
	s.state = SessionExited
	if m.activeSessions > 0 {
		m.activeSessions--
	}
	attachments := make([]*Attachment, 0, len(s.attachments))
	failedAttachments := make([]*Attachment, 0)
	for _, a := range s.attachments {
		attachments = append(attachments, a)
		if err := a.enqueueExitLocked(status); err != nil {
			failedAttachments = append(failedAttachments, a)
		}
	}
	failedObservers := m.emitLocked(Event{Kind: EventExited, Session: s.infoLocked()})
	m.mu.Unlock()
	for _, a := range failedAttachments {
		a.finish(ErrAttachmentClosed, AttachmentContextCanceled)
	}
	finishObservers(failedObservers, ErrQueueOverflow, ObserverQueueOverflow)
	attachmentsDone := allAttachmentsDone(attachments)
	if m.config.AttachmentDrainTimeout > 0 {
		timer := time.NewTimer(m.config.AttachmentDrainTimeout)
		select {
		case <-attachmentsDone:
			timer.Stop()
		case <-timer.C:
			for _, a := range attachments {
				a.finish(context.DeadlineExceeded, AttachmentDrainTimeout)
			}
			<-attachmentsDone
		}
	} else {
		for _, a := range attachments {
			a.finish(context.DeadlineExceeded, AttachmentDrainTimeout)
		}
		<-attachmentsDone
	}
	m.mu.Lock()
	clear(s.attachments)
	var removals []retainedRemoval
	var transitionFailed []*Subscription
	if !m.closing && m.config.SessionLifecycle.ExitPolicy == ExitedSessionRetain {
		s.retained = true
		m.retainedExited = append(m.retainedExited, s.id)
		m.generation++
		transitionFailed = m.emitLocked(Event{Kind: EventRetained, Session: s.infoLocked()})
		removals = m.beginRetentionEvictionsLocked()
	} else {
		m.removeSessionLocked(s)
		m.sessionsRemoved++
		transitionFailed = m.emitLocked(Event{Kind: EventRemoved, Session: s.infoLocked()})
	}
	m.mu.Unlock()
	finishObservers(transitionFailed, ErrQueueOverflow, ObserverQueueOverflow)
	for _, removal := range removals {
		m.finishRetainedRemoval(removal)
	}
	close(s.done)
}

func (m *Manager) beginRetentionEvictionsLocked() []retainedRemoval {
	var removals []retainedRemoval
	for len(m.retainedExited) > m.config.SessionLifecycle.MaxRetainedSessions {
		id := m.retainedExited[0]
		s, ok := m.sessions[id]
		if !ok || !s.retained {
			m.retainedExited = m.retainedExited[1:]
			continue
		}
		removal, ok := m.beginRetainedRemovalLocked(s, AttachmentSessionEvicted, EventEvicted)
		if ok {
			removals = append(removals, removal)
		}
	}
	return removals
}

func (m *Manager) beginRetainedRemovalLocked(s *Session, reason AttachmentCloseReason, eventKind EventKind) (retainedRemoval, bool) {
	if !s.retained || s.removing {
		return retainedRemoval{}, false
	}
	attachments := make([]*Attachment, 0, len(s.attachments))
	for _, attachment := range s.attachments {
		attachments = append(attachments, attachment)
	}
	clear(s.attachments)
	m.removeRetainedIDLocked(s.id)
	s.retained = false
	s.removing = true
	return retainedRemoval{session: s, attachments: attachments, attachmentReason: reason, eventKind: eventKind}, true
}

func (m *Manager) finishRetainedRemoval(removal retainedRemoval) {
	for _, attachment := range removal.attachments {
		attachment.finish(ErrSessionNotFound, removal.attachmentReason)
	}
	for _, attachment := range removal.attachments {
		<-attachment.Done()
	}
	m.mu.Lock()
	s, ok := m.sessions[removal.session.id]
	if !ok || s != removal.session || !s.removing {
		m.mu.Unlock()
		return
	}
	m.removeSessionLocked(s)
	if removal.eventKind == EventEvicted {
		m.sessionsEvicted++
	} else {
		m.sessionsRemoved++
	}
	failedObservers := m.emitLocked(Event{Kind: removal.eventKind, Session: s.infoLocked()})
	m.mu.Unlock()
	finishObservers(failedObservers, ErrQueueOverflow, ObserverQueueOverflow)
}

func (m *Manager) removeSessionLocked(s *Session) {
	delete(m.sessions, s.id)
	m.removeRetainedIDLocked(s.id)
	s.retained = false
	s.removing = false
	m.removeHistoryLocked(s)
	m.generation++
}

func (m *Manager) removeRetainedIDLocked(id streammux.StreamID) {
	for index, retainedID := range m.retainedExited {
		if retainedID == id {
			m.retainedExited = append(m.retainedExited[:index], m.retainedExited[index+1:]...)
			return
		}
	}
}

func (s *Session) pumpOutput() error {
	buffer := make([]byte, s.manager.config.ReadBufferBytes)
	for {
		n, err := s.process.Output().Read(buffer)
		if n < 0 || n > len(buffer) {
			return fmt.Errorf("pty: invalid read count %d", n)
		}
		if n > 0 {
			s.publish(append([]byte(nil), buffer[:n]...))
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
}

func (s *Session) publish(data []byte) {
	m := s.manager
	m.mu.Lock()
	s.lastSequence++
	event := OutputEvent{Sequence: s.lastSequence, Data: data}
	m.appendHistoryLocked(s, event)
	failed := make([]*Attachment, 0)
	for id, a := range s.attachments {
		if err := a.enqueueLocked(AttachmentEvent{Kind: AttachmentOutput, Output: &event}); err != nil {
			delete(s.attachments, id)
			failed = append(failed, a)
		}
	}
	failedObservers := m.emitLocked(Event{Kind: EventOutput, Session: s.infoLocked(), Output: &event})
	m.mu.Unlock()
	for _, a := range failed {
		a.finish(ErrQueueOverflow, AttachmentQueueOverflow)
	}
	finishObservers(failedObservers, ErrQueueOverflow, ObserverQueueOverflow)
}

func (m *Manager) appendHistoryLocked(s *Session, event OutputEvent) {
	if m.config.HistoryBytes == 0 || m.config.MaxTotalHistoryBytes == 0 {
		return
	}
	limit := m.config.HistoryBytes
	if int64(limit) > m.config.MaxTotalHistoryBytes {
		limit = int(m.config.MaxTotalHistoryBytes)
	}
	if len(event.Data) > limit {
		m.historyTruncatedBytes += uint64(len(event.Data) - limit)
		event.Data = append([]byte(nil), event.Data[len(event.Data)-limit:]...)
		m.historyTruncations++
	}
	m.nextHistory++
	chunk := historyChunk{OutputEvent: event, order: m.nextHistory}
	s.history = append(s.history, chunk)
	s.historyBytes += len(event.Data)
	m.historyBytes += int64(len(event.Data))
	for s.historyBytes > m.config.HistoryBytes && len(s.history) > 0 {
		m.dropFirstHistoryLocked(s)
	}
	for m.historyBytes > m.config.MaxTotalHistoryBytes {
		var oldest *Session
		var order uint64
		for _, candidate := range m.sessions {
			if len(candidate.history) > 0 && (oldest == nil || candidate.history[0].order < order) {
				oldest, order = candidate, candidate.history[0].order
			}
		}
		if oldest == nil {
			break
		}
		m.dropFirstHistoryLocked(oldest)
	}
}

func (m *Manager) dropFirstHistoryLocked(s *Session) {
	size := len(s.history[0].Data)
	s.history = s.history[1:]
	s.historyBytes -= size
	m.historyBytes -= int64(size)
	m.historyTruncations++
	m.historyTruncatedBytes += uint64(size)
}
func (m *Manager) removeHistoryLocked(s *Session) {
	m.historyBytes -= int64(s.historyBytes)
	s.history = nil
	s.historyBytes = 0
}

// Attachment owns one sink worker and its bounded ordered queue. It does not
// own or stop its Session.
type Attachment struct {
	session    *Session
	id         uint64
	sink       OutputSink
	maxBytes   int
	mu         sync.Mutex
	queue      []AttachmentEvent
	bytes      int
	active     bool
	closing    bool
	closed     bool
	err        error
	reason     AttachmentCloseReason
	changed    chan struct{}
	done       chan struct{}
	finishOnce sync.Once
	doneOnce   sync.Once
	started    bool
	registered bool
	ctx        context.Context
	cancel     context.CancelFunc
	stopParent func() bool
}

func newAttachment(parent context.Context, s *Session, id uint64, sink OutputSink, maxBytes int, active bool) *Attachment {
	ctx, cancel := context.WithCancel(s.manager.ctx)
	a := &Attachment{session: s, id: id, sink: sink, maxBytes: maxBytes, active: active, changed: make(chan struct{}), done: make(chan struct{}), ctx: ctx, cancel: cancel}
	a.stopParent = context.AfterFunc(parent, func() { a.close(AttachmentContextCanceled) })
	return a
}
func (a *Attachment) start() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return false
	}
	a.started = true
	return true
}

// ID returns the Manager-unique attachment identifier.
func (a *Attachment) ID() uint64 { return a.id }

// Done closes after the sink worker has stopped and close statistics are
// recorded.
func (a *Attachment) Done() <-chan struct{} { return a.done }

// Err returns the sink or overflow error that closed the attachment.
func (a *Attachment) Err() error { a.mu.Lock(); defer a.mu.Unlock(); return a.err }

// Reason returns the semantic reason recorded when the attachment closed.
func (a *Attachment) Reason() AttachmentCloseReason {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.reason
}

// Activate starts delivery for an attachment created with AttachOptions.Paused.
func (a *Attachment) Activate() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrAttachmentClosed
	}
	if !a.active {
		a.active = true
		a.signalLocked()
	}
	return nil
}

// Close is idempotent, discards queued events, and detaches without affecting
// the Session.
func (a *Attachment) Close() error {
	a.close(AttachmentClosedExplicitly)
	return nil
}
func (a *Attachment) close(reason AttachmentCloseReason) {
	a.finish(nil, reason)
}
func (a *Attachment) enqueueLocked(event AttachmentEvent) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.closing {
		return ErrAttachmentClosed
	}
	size := attachmentEventBytes(event)
	if size > a.maxBytes || a.bytes+size > a.maxBytes {
		return ErrQueueOverflow
	}
	a.queue = append(a.queue, event)
	a.bytes += size
	a.signalLocked()
	return nil
}
func (a *Attachment) enqueueExitLocked(status ExitStatus) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.closing {
		return ErrAttachmentClosed
	}
	value := status
	a.queue = append(a.queue, AttachmentEvent{Kind: AttachmentExit, Exit: &value})
	a.closing = true
	a.signalLocked()
	return nil
}
func (a *Attachment) signalLocked() { close(a.changed); a.changed = make(chan struct{}) }
func (a *Attachment) finish(err error, reason AttachmentCloseReason) {
	a.finishOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		a.err = err
		a.reason = reason
		a.queue = nil
		a.bytes = 0
		a.signalLocked()
		started := a.started
		a.mu.Unlock()
		a.cancel()
		if a.stopParent != nil {
			a.stopParent()
		}
		a.session.manager.recordAttachmentClose(a, reason)
		if !started {
			a.closeDone()
		}
	})
}
func (a *Attachment) run() {
	defer a.session.manager.workers.Done()
	defer a.closeDone()
	for {
		a.mu.Lock()
		if a.closed {
			a.mu.Unlock()
			return
		}
		if a.closing && len(a.queue) == 0 {
			a.mu.Unlock()
			a.finish(nil, AttachmentSessionExited)
			return
		}
		if !a.active || len(a.queue) == 0 {
			changed := a.changed
			a.mu.Unlock()
			select {
			case <-changed:
				continue
			case <-a.ctx.Done():
				reason := AttachmentContextCanceled
				if a.session.manager.ctx.Err() != nil {
					reason = AttachmentManagerClosed
				}
				a.close(reason)
				return
			}
		}
		event := a.queue[0]
		a.queue = a.queue[1:]
		a.bytes -= attachmentEventBytes(event)
		a.signalLocked()
		a.mu.Unlock()
		if err := a.sink.Send(a.ctx, event); err != nil {
			reason := AttachmentSinkFailure
			if a.session.manager.ctx.Err() != nil {
				reason = AttachmentManagerClosed
			}
			a.finish(err, reason)
			return
		}
	}
}

func (a *Attachment) closeDone() {
	a.doneOnce.Do(func() { close(a.done) })
}

func attachmentEventBytes(event AttachmentEvent) int {
	if event.Output != nil {
		return len(event.Output.Data)
	}
	return 0
}

func allAttachmentsDone(attachments []*Attachment) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		for _, attachment := range attachments {
			<-attachment.Done()
		}
		close(done)
	}()
	return done
}

// Subscription owns one bounded observer queue. A slow subscriber is closed
// independently of sessions and other observers.
type Subscription struct {
	manager    *Manager
	id         uint64
	maxBytes   int
	mu         sync.Mutex
	queue      []Event
	bytes      int
	closed     bool
	err        error
	reason     ObserverCloseReason
	changed    chan struct{}
	done       chan struct{}
	events     chan Event
	ctx        context.Context
	cancel     context.CancelFunc
	finishOnce sync.Once
}

func newSubscription(m *Manager, id uint64, max int) *Subscription {
	ctx, cancel := context.WithCancel(m.ctx)
	return &Subscription{manager: m, id: id, maxBytes: max, changed: make(chan struct{}), done: make(chan struct{}), events: make(chan Event), ctx: ctx, cancel: cancel}
}

// Events returns the stable channel closed after the subscription worker exits.
func (s *Subscription) Events() <-chan Event { return s.events }
func (s *Subscription) next() (Event, bool) {
	for {
		s.mu.Lock()
		if len(s.queue) > 0 {
			e := s.queue[0]
			s.queue = s.queue[1:]
			s.bytes -= eventBytes(e)
			s.signalLocked()
			s.mu.Unlock()
			return e, true
		}
		if s.closed {
			s.mu.Unlock()
			return Event{}, false
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-s.ctx.Done():
			return Event{}, false
		}
	}
}
func (s *Subscription) run() {
	defer s.manager.workers.Done()
	defer close(s.events)
	for {
		event, ok := s.next()
		if !ok {
			return
		}
		select {
		case s.events <- event:
		case <-s.ctx.Done():
			return
		}
	}
}

// Done closes when close statistics have been recorded.
func (s *Subscription) Done() <-chan struct{} { return s.done }

// Err returns the overflow error that closed the subscription.
func (s *Subscription) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }

// Reason returns the semantic reason recorded when the subscription closed.
func (s *Subscription) Reason() ObserverCloseReason {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reason
}

// Close is idempotent and discards queued observer events.
func (s *Subscription) Close() error {
	s.manager.mu.Lock()
	delete(s.manager.observers, s.id)
	s.manager.mu.Unlock()
	s.finish(nil, ObserverClosedExplicitly)
	return nil
}
func (s *Subscription) enqueue(event Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrObserverClosed
	}
	size := eventBytes(event)
	if size > s.maxBytes || s.bytes+size > s.maxBytes {
		return ErrQueueOverflow
	}
	s.queue = append(s.queue, event)
	s.bytes += size
	s.signalLocked()
	return nil
}
func (s *Subscription) signalLocked() { close(s.changed); s.changed = make(chan struct{}) }
func (s *Subscription) finish(err error, reason ObserverCloseReason) {
	s.finishOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.err = err
		s.reason = reason
		s.queue = nil
		s.bytes = 0
		s.signalLocked()
		s.mu.Unlock()
		s.cancel()
		s.manager.recordObserverClose(reason)
		close(s.done)
	})
}
func eventBytes(event Event) int {
	if event.Output != nil {
		return len(event.Output.Data) + 64
	}
	return 128
}
func (m *Manager) emitLocked(event Event) []*Subscription {
	failed := make([]*Subscription, 0)
	for id, s := range m.observers {
		if err := s.enqueue(event); err != nil {
			delete(m.observers, id)
			failed = append(failed, s)
		}
	}
	return failed
}

func finishObservers(observers []*Subscription, err error, reason ObserverCloseReason) {
	for _, observer := range observers {
		observer.finish(err, reason)
	}
}

func (m *Manager) recordAttachmentClose(attachment *Attachment, reason AttachmentCloseReason) {
	m.mu.Lock()
	registered := attachment.registered
	if registered {
		attachment.registered = false
		delete(attachment.session.attachments, attachment.id)
		m.attachmentClosures[reason]++
	}
	var failedObservers []*Subscription
	if registered {
		failedObservers = m.emitLocked(Event{Kind: EventDetached, Session: attachment.session.infoLocked(), Attachment: &AttachmentInfo{ID: attachment.id, Reason: reason}})
	}
	m.mu.Unlock()
	finishObservers(failedObservers, ErrQueueOverflow, ObserverQueueOverflow)
}

func (m *Manager) recordObserverClose(reason ObserverCloseReason) {
	m.mu.Lock()
	m.observerClosures[reason]++
	m.mu.Unlock()
}

func (m *Manager) recordProcessExited() {
	m.mu.Lock()
	m.processesExited++
	m.mu.Unlock()
}
