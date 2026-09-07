package streammux

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

var (
	ErrNilConn              = errors.New("streammux: nil connection")
	ErrPeerClosed           = errors.New("streammux: peer closed")
	ErrPeerAlreadyServing   = errors.New("streammux: peer Serve called more than once")
	ErrHandlerRegistered    = errors.New("streammux: message handler already registered")
	ErrHandlerNotFound      = errors.New("streammux: message handler not found")
	ErrUnexpectedResponse   = errors.New("streammux: response has no pending request")
	ErrPeerQueueOverflow    = errors.New("streammux: peer queue limit exceeded")
	ErrTooManyActiveStreams = errors.New("streammux: too many active streams")
)

// TrafficClass controls outbound scheduling priority. Per-stream order is
// preserved even when successive frames use different classes.
type TrafficClass uint8

const (
	TrafficControl TrafficClass = iota + 1
	TrafficInteractive
	TrafficBulk
)

// PeerConfig bounds Peer resources and configures weighted scheduling. Zero
// fields use DefaultPeerConfig values. Classify is called synchronously by
// Emit without internal locks held and must be safe for concurrent use.
type PeerConfig struct {
	OutboundQueueBytes   int
	OutboundQueueFrames  int
	PerStreamQueueBytes  int
	PerStreamQueueFrames int
	MaxActiveStreams     int
	MaxCanceledRequests  int
	ControlWeight        int
	InteractiveWeight    int
	BulkWeight           int
	Classify             func(Frame) TrafficClass
}

// DefaultPeerConfig returns production-oriented bounded queue defaults.
func DefaultPeerConfig() PeerConfig {
	return PeerConfig{
		OutboundQueueBytes: 16 << 20, OutboundQueueFrames: 4096,
		PerStreamQueueBytes: 1 << 20, PerStreamQueueFrames: 256,
		MaxActiveStreams: 1024, MaxCanceledRequests: 4096,
		ControlWeight: 8, InteractiveWeight: 4, BulkWeight: 1,
	}
}

func (c PeerConfig) withDefaults() (PeerConfig, error) {
	d := DefaultPeerConfig()
	if c.OutboundQueueBytes == 0 {
		c.OutboundQueueBytes = d.OutboundQueueBytes
	}
	if c.PerStreamQueueBytes == 0 {
		c.PerStreamQueueBytes = d.PerStreamQueueBytes
	}
	if c.OutboundQueueFrames == 0 {
		c.OutboundQueueFrames = d.OutboundQueueFrames
	}
	if c.PerStreamQueueFrames == 0 {
		c.PerStreamQueueFrames = d.PerStreamQueueFrames
	}
	if c.MaxActiveStreams == 0 {
		c.MaxActiveStreams = d.MaxActiveStreams
	}
	if c.MaxCanceledRequests == 0 {
		c.MaxCanceledRequests = d.MaxCanceledRequests
	}
	if c.ControlWeight == 0 {
		c.ControlWeight = d.ControlWeight
	}
	if c.InteractiveWeight == 0 {
		c.InteractiveWeight = d.InteractiveWeight
	}
	if c.BulkWeight == 0 {
		c.BulkWeight = d.BulkWeight
	}
	if c.OutboundQueueBytes <= 0 || c.OutboundQueueFrames <= 0 || c.PerStreamQueueBytes <= 0 || c.PerStreamQueueFrames <= 0 || c.MaxActiveStreams <= 0 || c.MaxCanceledRequests <= 0 || c.ControlWeight <= 0 || c.InteractiveWeight <= 0 || c.BulkWeight <= 0 {
		return c, errors.New("streammux: invalid PeerConfig")
	}
	if c.PerStreamQueueBytes > c.OutboundQueueBytes {
		return c, errors.New("streammux: per-stream queue exceeds outbound queue")
	}
	if c.PerStreamQueueFrames > c.OutboundQueueFrames {
		return c, errors.New("streammux: per-stream frame queue exceeds outbound queue")
	}
	if c.Classify == nil {
		c.Classify = func(frame Frame) TrafficClass {
			if frame.Header.Flags != FlagEvent {
				return TrafficControl
			}
			return TrafficInteractive
		}
	}
	return c, nil
}

// Handler processes a request or event. Request handlers must call Respond
// exactly once. Handler execution is ordered per StreamID and may run in
// parallel across different StreamIDs. The context is canceled when Peer
// closes. Frame.Payload may be retained but must not be mutated.
type Handler func(context.Context, *Peer, Frame) error

type peerPending struct {
	messageType MessageType
	streamID    StreamID
	response    chan peerResult
}
type peerResult struct {
	frame Frame
	err   error
}

type inboundStream struct {
	frames []Frame
	bytes  int
}

type outboundRequest struct {
	frame  Frame
	class  TrafficClass
	bytes  int
	result chan error
}

type outboundStream struct {
	id    StreamID
	queue []*outboundRequest
	bytes int
}

// Peer adds request correlation, protocol dispatch and bounded scheduling on
// top of Conn. Conn remains available as the stable low-level API.
type Peer struct {
	conn   *Conn
	config PeerConfig

	mu             sync.Mutex
	handlers       map[MessageType]Handler
	pending        map[CorrelationID]*peerPending
	canceled       map[CorrelationID]peerPending
	canceledOrder  []CorrelationID
	inbound        map[StreamID]*inboundStream
	outbound       map[StreamID]*outboundStream
	outboundOrder  []StreamID
	outboundBytes  int
	outboundFrames int
	receivedFrames uint64
	receivedBytes  uint64
	sentFrames     uint64
	sentBytes      uint64
	protocolErrors uint64
	queueOverflows uint64
	changed        chan struct{}
	closed         bool
	err            error

	next      atomic.Uint64
	serveOnce atomic.Bool
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	workers   sync.WaitGroup
	scheduler sync.WaitGroup
}

// PeerStreamStats reports current bounded-queue use for one StreamID. It never
// contains payload contents.
type PeerStreamStats struct {
	StreamID            StreamID
	InboundQueueBytes   int
	InboundQueueFrames  int
	OutboundQueueBytes  int
	OutboundQueueFrames int
}

// PeerStats contains cumulative traffic/error counters and current resource
// use. Byte counters include the fixed frame header and never payload contents.
type PeerStats struct {
	ReceivedFrames        uint64
	ReceivedBytes         uint64
	SentFrames            uint64
	SentBytes             uint64
	ProtocolErrors        uint64
	QueueOverflows        uint64
	PendingRequests       int
	CanceledRequests      int
	ActiveInboundStreams  int
	ActiveOutboundStreams int
	InboundQueueBytes     int
	InboundQueueFrames    int
	OutboundQueueBytes    int
	OutboundQueueFrames   int
	Streams               []PeerStreamStats
}

// NewPeer takes ownership of conn on success and starts its outbound worker.
// Close must be called when Serve is not used through connection shutdown.
func NewPeer(conn *Conn, config PeerConfig) (*Peer, error) {
	if conn == nil {
		return nil, ErrNilConn
	}
	validated, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Peer{
		conn: conn, config: validated, handlers: make(map[MessageType]Handler),
		pending: make(map[CorrelationID]*peerPending), canceled: make(map[CorrelationID]peerPending), inbound: make(map[StreamID]*inboundStream),
		outbound: make(map[StreamID]*outboundStream), changed: make(chan struct{}),
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
	}
	p.scheduler.Add(1)
	go p.schedule()
	return p, nil
}

// Register adds one handler. Registration is safe before or during Serve;
// handlers cannot be replaced or removed.
func (p *Peer) Register(messageType MessageType, handler Handler) error {
	return p.RegisterHandlers(map[MessageType]Handler{messageType: handler})
}

// RegisterHandlers validates and commits a group of handlers atomically.
// This allows a protocol component to avoid partial registration on conflict.
func (p *Peer) RegisterHandlers(handlers map[MessageType]Handler) error {
	if len(handlers) == 0 {
		return ErrHandlerNotFound
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPeerClosed
	}
	for messageType, handler := range handlers {
		if messageType == 0 || handler == nil {
			return ErrHandlerNotFound
		}
		if _, exists := p.handlers[messageType]; exists {
			return fmt.Errorf("%w: %d", ErrHandlerRegistered, messageType)
		}
	}
	for messageType, handler := range handlers {
		p.handlers[messageType] = handler
	}
	return nil
}

// Call copies and sends one request, then waits for its matching response.
// Cancellation abandons only this call; a bounded tombstone safely absorbs a
// late response. The returned response payload is owned by the caller.
func (p *Peer) Call(ctx context.Context, frame Frame) (Frame, error) {
	if ctx == nil {
		return Frame{}, errors.New("streammux: nil call context")
	}
	if frame.Header.Flags != FlagRequest {
		return Frame{}, ErrInvalidFlags
	}
	correlation, err := p.reservePending()
	if err != nil {
		return Frame{}, err
	}
	frame.Header.CorrelationID = correlation
	p.mu.Lock()
	pending := p.pending[correlation]
	pending.messageType = frame.Header.MessageType
	pending.streamID = frame.Header.StreamID
	p.mu.Unlock()
	if err = p.send(ctx, frame.Clone(), TrafficControl); err != nil {
		p.abandonPending(correlation)
		return Frame{}, err
	}
	select {
	case result := <-pending.response:
		return result.frame, result.err
	case <-ctx.Done():
		p.abandonPending(correlation)
		return Frame{}, ctx.Err()
	case <-p.done:
		return Frame{}, p.result()
	}
}

// Emit copies and sends an event using the configured classifier. ctx bounds
// queue and transport waiting; it does not close Peer when canceled.
func (p *Peer) Emit(ctx context.Context, frame Frame) error {
	if frame.Header.Flags != FlagEvent || frame.Header.CorrelationID != 0 {
		return ErrInvalidFlags
	}
	return p.send(ctx, frame.Clone(), p.config.Classify(frame))
}

// Send copies frame.Payload before placing the frame in the selected traffic
// class. Prefer SendOwned when the caller can transfer ownership.
func (p *Peer) Send(ctx context.Context, frame Frame, class TrafficClass) error {
	return p.send(ctx, frame.Clone(), class)
}

// SendOwned transfers ownership of frame.Payload to Peer, including when an
// error is returned. The caller must never mutate or reuse that storage.
func (p *Peer) SendOwned(ctx context.Context, frame Frame, class TrafficClass) error {
	return p.send(ctx, frame, class)
}

// Respond copies and sends a response correlated to request.
func (p *Peer) Respond(ctx context.Context, request Frame, response Frame) error {
	if request.Header.Flags != FlagRequest {
		return ErrInvalidFlags
	}
	response.Header.Flags = FlagResponse
	response.Header.CorrelationID = request.Header.CorrelationID
	response.Header.StreamID = request.Header.StreamID
	if response.Header.MessageType == 0 {
		response.Header.MessageType = request.Header.MessageType
	}
	return p.send(ctx, response.Clone(), TrafficControl)
}

// Serve is the sole Conn reader and may be called exactly once. It returns
// after ctx cancellation, transport failure, or a protocol/handler failure;
// each of those terminal conditions closes Peer and cancels all handlers.
func (p *Peer) Serve(ctx context.Context) error {
	if ctx == nil {
		return errors.New("streammux: nil serve context")
	}
	if !p.serveOnce.CompareAndSwap(false, true) {
		return ErrPeerAlreadyServing
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			p.fail(ctx.Err())
		case <-done:
		}
	}()
	for {
		frame, err := p.conn.ReadFrame()
		if err != nil {
			if isPeerProtocolError(err) {
				p.recordProtocolError()
			}
			p.fail(err)
			return p.result()
		}
		p.recordReceived(frame)
		if frame.Header.Flags == FlagResponse {
			if err = p.resolve(frame); err != nil {
				p.recordProtocolError()
				p.fail(err)
				return err
			}
			continue
		}
		if err = p.dispatch(frame); err != nil {
			p.recordProtocolError()
			p.fail(err)
			return err
		}
	}
}

func (p *Peer) dispatch(frame Frame) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPeerClosed
	}
	handler := p.handlers[frame.Header.MessageType]
	if handler == nil {
		p.mu.Unlock()
		return fmt.Errorf("%w: %d", ErrHandlerNotFound, frame.Header.MessageType)
	}
	state := p.inbound[frame.Header.StreamID]
	if state == nil {
		if len(p.inbound) >= p.config.MaxActiveStreams {
			p.mu.Unlock()
			return ErrTooManyActiveStreams
		}
		state = &inboundStream{}
		p.inbound[frame.Header.StreamID] = state
		p.workers.Add(1)
		go p.runInbound(frame.Header.StreamID, state)
	}
	bytes := len(frame.Payload) + HeaderSize
	if state.bytes+bytes > p.config.PerStreamQueueBytes || len(state.frames)+1 > p.config.PerStreamQueueFrames {
		p.queueOverflows++
		p.mu.Unlock()
		return ErrPeerQueueOverflow
	}
	state.bytes += bytes
	state.frames = append(state.frames, frame)
	p.mu.Unlock()
	return nil
}

func (p *Peer) runInbound(id StreamID, state *inboundStream) {
	defer p.workers.Done()
	for {
		p.mu.Lock()
		if len(state.frames) > 0 {
			frame := state.frames[0]
			state.frames = state.frames[1:]
			state.bytes -= len(frame.Payload) + HeaderSize
			handler := p.handlers[frame.Header.MessageType]
			p.signalLocked()
			p.mu.Unlock()
			if handler == nil {
				p.recordProtocolError()
				p.fail(ErrHandlerNotFound)
				return
			}
			if err := callHandler(p.ctx, handler, p, frame); err != nil {
				p.recordProtocolError()
				p.fail(err)
				return
			}
			continue
		}
		if p.inbound[id] == state {
			delete(p.inbound, id)
		}
		p.mu.Unlock()
		return
	}
}

func callHandler(ctx context.Context, handler Handler, peer *Peer, frame Frame) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("streammux: handler panic: %v", recovered)
		}
	}()
	return handler(ctx, peer, frame)
}

func (p *Peer) reservePending() (CorrelationID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, ErrPeerClosed
	}
	for attempts := uint64(0); attempts < ^uint64(0); attempts++ {
		id := CorrelationID(p.next.Add(1))
		if id == 0 {
			continue
		}
		_, pending := p.pending[id]
		_, canceled := p.canceled[id]
		if !pending && !canceled {
			p.pending[id] = &peerPending{response: make(chan peerResult, 1)}
			return id, nil
		}
	}
	return 0, ErrInvalidCorrelation
}

func (p *Peer) abandonPending(id CorrelationID) {
	p.mu.Lock()
	pending := p.pending[id]
	if pending != nil {
		delete(p.pending, id)
		p.canceled[id] = *pending
		p.canceledOrder = append(p.canceledOrder, id)
		if len(p.canceledOrder) > p.config.MaxCanceledRequests {
			oldest := p.canceledOrder[0]
			p.canceledOrder = p.canceledOrder[1:]
			delete(p.canceled, oldest)
		}
	}
	p.mu.Unlock()
}

func (p *Peer) resolve(frame Frame) error {
	p.mu.Lock()
	pending := p.pending[frame.Header.CorrelationID]
	if pending != nil && pending.messageType == frame.Header.MessageType && pending.streamID == frame.Header.StreamID {
		delete(p.pending, frame.Header.CorrelationID)
	} else if pending != nil {
		p.mu.Unlock()
		return ErrUnexpectedResponse
	}
	if pending == nil {
		canceled, ok := p.canceled[frame.Header.CorrelationID]
		if ok && canceled.messageType == frame.Header.MessageType && canceled.streamID == frame.Header.StreamID {
			delete(p.canceled, frame.Header.CorrelationID)
			for index, id := range p.canceledOrder {
				if id == frame.Header.CorrelationID {
					p.canceledOrder = append(p.canceledOrder[:index], p.canceledOrder[index+1:]...)
					break
				}
			}
			p.mu.Unlock()
			return nil
		}
	}
	p.mu.Unlock()
	if pending == nil {
		return ErrUnexpectedResponse
	}
	pending.response <- peerResult{frame: frame}
	return nil
}

func (p *Peer) send(ctx context.Context, frame Frame, class TrafficClass) error {
	if ctx == nil {
		return errors.New("streammux: nil send context")
	}
	if class < TrafficControl || class > TrafficBulk {
		return errors.New("streammux: invalid traffic class")
	}
	request := &outboundRequest{frame: frame, class: class, bytes: len(frame.Payload) + HeaderSize, result: make(chan error, 1)}
	if request.bytes > p.config.PerStreamQueueBytes || request.bytes > p.config.OutboundQueueBytes {
		p.mu.Lock()
		p.queueOverflows++
		p.mu.Unlock()
		return ErrPeerQueueOverflow
	}
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return p.result()
		}
		state := p.outbound[frame.Header.StreamID]
		active := len(p.outbound)
		fits := request.bytes <= p.config.PerStreamQueueBytes && request.bytes <= p.config.OutboundQueueBytes
		if state == nil && active >= p.config.MaxActiveStreams {
			fits = false
		}
		if state != nil && (state.bytes+request.bytes > p.config.PerStreamQueueBytes || len(state.queue)+1 > p.config.PerStreamQueueFrames) {
			fits = false
		}
		if p.outboundBytes+request.bytes > p.config.OutboundQueueBytes || p.outboundFrames+1 > p.config.OutboundQueueFrames {
			fits = false
		}
		if fits {
			if state == nil {
				state = &outboundStream{id: frame.Header.StreamID}
				p.outbound[frame.Header.StreamID] = state
				p.outboundOrder = append(p.outboundOrder, frame.Header.StreamID)
			}
			state.queue = append(state.queue, request)
			state.bytes += request.bytes
			p.outboundBytes += request.bytes
			p.outboundFrames++
			p.signalLocked()
			p.mu.Unlock()
			break
		}
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-p.done:
			return p.result()
		}
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return p.result()
	}
}

func (p *Peer) schedule() {
	defer p.scheduler.Done()
	classes := []TrafficClass{TrafficControl, TrafficInteractive, TrafficBulk}
	weights := map[TrafficClass]int{TrafficControl: p.config.ControlWeight, TrafficInteractive: p.config.InteractiveWeight, TrafficBulk: p.config.BulkWeight}
	classIndex, credit, cursor := 0, 0, 0
	for {
		p.mu.Lock()
		var request *outboundRequest
		for scans := 0; scans < len(classes)*2 && request == nil; scans++ {
			class := classes[classIndex]
			if credit == 0 {
				credit = weights[class]
			}
			request, cursor = p.takeLocked(class, cursor)
			if request != nil {
				credit--
			}
			if request == nil || credit == 0 {
				classIndex = (classIndex + 1) % len(classes)
				credit = 0
			}
		}
		if request == nil {
			if p.closed {
				p.mu.Unlock()
				return
			}
			changed := p.changed
			p.mu.Unlock()
			select {
			case <-changed:
				continue
			case <-p.done:
				return
			}
		}
		p.mu.Unlock()
		err := p.conn.SendOwned(p.ctx, request.frame)
		if err == nil {
			p.recordSent(request.frame)
		}
		request.result <- err
		if err != nil {
			p.fail(err)
			return
		}
	}
}

func (p *Peer) takeLocked(class TrafficClass, cursor int) (*outboundRequest, int) {
	if len(p.outboundOrder) == 0 {
		return nil, 0
	}
	for checked := 0; checked < len(p.outboundOrder); checked++ {
		index := (cursor + checked) % len(p.outboundOrder)
		state := p.outbound[p.outboundOrder[index]]
		if state == nil || len(state.queue) == 0 || state.queue[0].class != class {
			continue
		}
		request := state.queue[0]
		state.queue = state.queue[1:]
		state.bytes -= request.bytes
		p.outboundBytes -= request.bytes
		p.outboundFrames--
		next := (index + 1) % len(p.outboundOrder)
		if len(state.queue) == 0 {
			delete(p.outbound, state.id)
			p.outboundOrder = append(p.outboundOrder[:index], p.outboundOrder[index+1:]...)
			if len(p.outboundOrder) == 0 {
				next = 0
			} else if index < next {
				next--
			}
		}
		p.signalLocked()
		return request, next
	}
	return nil, cursor
}

func (p *Peer) signalLocked() { close(p.changed); p.changed = make(chan struct{}) }

func (p *Peer) fail(err error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.err = err
	for _, pending := range p.pending {
		pending.response <- peerResult{err: err}
	}
	clear(p.pending)
	clear(p.canceled)
	p.canceledOrder = nil
	for _, state := range p.outbound {
		for _, request := range state.queue {
			request.result <- err
		}
	}
	clear(p.outbound)
	p.outboundOrder = nil
	p.outboundBytes = 0
	p.outboundFrames = 0
	p.signalLocked()
	close(p.done)
	p.mu.Unlock()
	p.cancel()
	_ = p.conn.Close()
}

// Close is idempotent. It closes the underlying Conn, cancels pending work,
// and waits for the scheduler and all handlers to return.
func (p *Peer) Close() error {
	p.fail(ErrPeerClosed)
	p.scheduler.Wait()
	p.workers.Wait()
	err := p.result()
	if errors.Is(err, ErrPeerClosed) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// Stats reports bounded-queue and activity counters without exposing mutable
// internal state.
func (p *Peer) Stats() PeerStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	stats := PeerStats{
		ReceivedFrames: p.receivedFrames, ReceivedBytes: p.receivedBytes,
		SentFrames: p.sentFrames, SentBytes: p.sentBytes,
		ProtocolErrors: p.protocolErrors, QueueOverflows: p.queueOverflows,
		PendingRequests: len(p.pending), CanceledRequests: len(p.canceled), ActiveInboundStreams: len(p.inbound),
		ActiveOutboundStreams: len(p.outbound), OutboundQueueBytes: p.outboundBytes,
		OutboundQueueFrames: p.outboundFrames,
	}
	streams := make(map[StreamID]PeerStreamStats, len(p.inbound)+len(p.outbound))
	for id, stream := range p.inbound {
		stats.InboundQueueBytes += stream.bytes
		stats.InboundQueueFrames += len(stream.frames)
		item := streams[id]
		item.StreamID = id
		item.InboundQueueBytes = stream.bytes
		item.InboundQueueFrames = len(stream.frames)
		streams[id] = item
	}
	for id, stream := range p.outbound {
		item := streams[id]
		item.StreamID = id
		item.OutboundQueueBytes = stream.bytes
		item.OutboundQueueFrames = len(stream.queue)
		streams[id] = item
	}
	stats.Streams = make([]PeerStreamStats, 0, len(streams))
	for _, item := range streams {
		stats.Streams = append(stats.Streams, item)
	}
	sort.Slice(stats.Streams, func(i, j int) bool { return stats.Streams[i].StreamID < stats.Streams[j].StreamID })
	return stats
}

func (p *Peer) recordReceived(frame Frame) {
	p.mu.Lock()
	p.receivedFrames++
	p.receivedBytes += uint64(HeaderSize + len(frame.Payload))
	p.mu.Unlock()
}

func (p *Peer) recordSent(frame Frame) {
	p.mu.Lock()
	p.sentFrames++
	p.sentBytes += uint64(HeaderSize + len(frame.Payload))
	p.mu.Unlock()
}

func (p *Peer) recordProtocolError() {
	p.mu.Lock()
	p.protocolErrors++
	p.mu.Unlock()
}

func isPeerProtocolError(err error) bool {
	return errors.Is(err, ErrFrameTooLarge) || errors.Is(err, ErrInvalidFrameLength) ||
		errors.Is(err, ErrUnsupportedVersion) || errors.Is(err, ErrInvalidMessageType) ||
		errors.Is(err, ErrInvalidFlags) || errors.Is(err, ErrInvalidCorrelation)
}

// MaxFrameBytes returns the payload limit enforced by the underlying Conn.
func (p *Peer) MaxFrameBytes() int { return p.conn.config.MaxFrameBytes }

// Done closes when Peer begins terminal shutdown. Handler goroutines may still
// be returning; Close waits for them.
func (p *Peer) Done() <-chan struct{} { return p.done }

// Err returns the first terminal Peer error, or nil before shutdown.
func (p *Peer) Err() error { p.mu.Lock(); defer p.mu.Unlock(); return p.err }
func (p *Peer) result() error {
	if err := p.Err(); err != nil {
		return err
	}
	return ErrPeerClosed
}
