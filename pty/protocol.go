package pty

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aruzen/streammux"
)

// ErrUnsupportedProtocolVersion indicates a control payload version mismatch.
var ErrUnsupportedProtocolVersion = errors.New("pty: unsupported protocol version")

// MessageTypes assigns application-owned, distinct message type numbers to
// every PTY operation and event.
type MessageTypes struct {
	Open        streammux.MessageType
	List        streammux.MessageType
	Inspect     streammux.MessageType
	Attach      streammux.MessageType
	Detach      streammux.MessageType
	Input       streammux.MessageType
	Output      streammux.MessageType
	Resize      streammux.MessageType
	Kill        streammux.MessageType
	Exit        streammux.MessageType
	ReplayBegin streammux.MessageType
	ReplayEnd   streammux.MessageType
	Error       streammux.MessageType
}

func (t MessageTypes) validate() error {
	values := []streammux.MessageType{t.Open, t.List, t.Inspect, t.Attach, t.Detach, t.Input, t.Output, t.Resize, t.Kill, t.Exit, t.ReplayBegin, t.ReplayEnd, t.Error}
	seen := make(map[streammux.MessageType]struct{}, len(values))
	for _, value := range values {
		if value == 0 {
			return ErrInvalidMessageTypes
		}
		if _, ok := seen[value]; ok {
			return ErrInvalidMessageTypes
		}
		seen[value] = struct{}{}
	}
	return nil
}

// ProtocolConfig configures one connection-scoped protocol adapter. Zero
// limits use DefaultProtocolConfig. Authorization and filtering callbacks run
// without Manager locks and must honor their context.
type ProtocolConfig struct {
	Version           uint16
	Types             MessageTypes
	MaxListItems      int
	MaxCommandBytes   int
	MaxDirectoryBytes int
	MaxArgumentBytes  int
	MaxArguments      int
	MaxEnvironment    int
	Authorize         func(context.Context, AuthorizationRequest) error
	FilterSessions    func(context.Context, []SessionInfo) ([]SessionInfo, error)
}

// Operation identifies a semantic PTY protocol action for authorization.
type Operation string

const (
	OperationOpen    Operation = "open"
	OperationList    Operation = "list"
	OperationInspect Operation = "inspect"
	OperationAttach  Operation = "attach"
	OperationDetach  Operation = "detach"
	OperationInput   Operation = "input"
	OperationResize  Operation = "resize"
	OperationKill    Operation = "kill"
)

// AuthorizationRequest deliberately excludes terminal input/output bytes.
// ProcessSpec is populated only for Open.
type AuthorizationRequest struct {
	Operation   Operation
	SessionID   streammux.StreamID
	ProcessSpec *ProcessSpec
}

// DefaultProtocolConfig returns version 1 and bounded control payload limits.
func DefaultProtocolConfig() ProtocolConfig {
	return ProtocolConfig{
		Version: 1, MaxListItems: 128, MaxCommandBytes: 4096,
		MaxDirectoryBytes: 16 << 10, MaxArgumentBytes: 64 << 10,
		MaxArguments: 256, MaxEnvironment: 256,
	}
}

// ErrorCode is a stable machine-readable remote failure category.
type ErrorCode string

const (
	CodeInvalidRequest     ErrorCode = "invalid_request"
	CodeSessionNotFound    ErrorCode = "session_not_found"
	CodeSessionExists      ErrorCode = "session_exists"
	CodeAlreadyAttached    ErrorCode = "already_attached"
	CodeAttachmentLimit    ErrorCode = "attachment_limit"
	CodeSessionExited      ErrorCode = "session_exited"
	CodeManagerClosed      ErrorCode = "manager_closed"
	CodeUnsupportedVersion ErrorCode = "unsupported_version"
	CodeQueueOverflow      ErrorCode = "queue_overflow"
	CodeBackendFailure     ErrorCode = "backend_failure"
	CodePermissionDenied   ErrorCode = "permission_denied"
)

// RemoteError is safe to expose to a remote client and supports errors.Is by
// ErrorCode. Details must never contain terminal payload bytes.
type RemoteError struct {
	Code    ErrorCode         `json:"code"`
	Message string            `json:"message"`
	Details map[string]string `json:"details,omitempty"`
}

func (e *RemoteError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("pty remote %s: %s", e.Code, e.Message)
}
func (e *RemoteError) Is(target error) bool {
	other, ok := target.(*RemoteError)
	return ok && other.Code == e.Code
}

// ProtocolResponse is the common response envelope used by the PTY protocol.
// Exactly the fields relevant to the request are populated.
type ProtocolResponse struct {
	Version    uint16        `json:"version"`
	Error      *RemoteError  `json:"error,omitempty"`
	Session    *SessionInfo  `json:"session,omitempty"`
	Sessions   []SessionInfo `json:"sessions,omitempty"`
	NextCursor string        `json:"next_cursor,omitempty"`
	Attach     *AttachResult `json:"attach,omitempty"`
}

// OpenRequest starts one managed PTY session. StreamID must be zero; the
// allocated session ID is returned in ProtocolResponse.Session.
type OpenRequest struct {
	Version     uint16   `json:"version"`
	Command     string   `json:"command"`
	Args        []string `json:"args,omitempty"`
	Env         []string `json:"env,omitempty"`
	Dir         string   `json:"dir,omitempty"`
	InitialSize Size     `json:"initial_size"`
}

// AttachRequest selects the history replay behavior for an attachment.
type AttachRequest struct {
	Version     uint16     `json:"version"`
	Replay      ReplayMode `json:"replay,omitempty"`
	ResumeAfter uint64     `json:"resume_after,omitempty"`
}

// ListRequest requests one stable-generation page of sessions.
type ListRequest struct {
	Version uint16 `json:"version"`
	Cursor  string `json:"cursor,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

// ReplayMetadata describes the replay interval preceding live output.
type ReplayMetadata struct {
	Version   uint16 `json:"version"`
	First     uint64 `json:"first"`
	Last      uint64 `json:"last"`
	NextLive  uint64 `json:"next_live"`
	Truncated bool   `json:"truncated,omitempty"`
}

// ProtocolErrorEvent reports an asynchronous attachment failure. It contains
// no terminal input or output bytes. The sending Peer is closed after a
// best-effort attempt to deliver this event.
type ProtocolErrorEvent struct {
	Version uint16    `json:"version"`
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

// EncodeControl encodes a versioned PTY control payload as JSON.
func EncodeControl(value any) ([]byte, error) { return json.Marshal(value) }

// DecodeProtocolResponse decodes a PTY response and returns its structured
// remote error as error without closing the underlying Peer.
func DecodeProtocolResponse(payload []byte) (ProtocolResponse, error) {
	var response ProtocolResponse
	if len(payload) == 0 {
		return response, ErrInvalidPayload
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		return response, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	if response.Error != nil {
		return response, response.Error
	}
	return response, nil
}

// Protocol binds PTY handlers and connection-local attachments to one Peer.
// It never owns or closes Manager sessions.
type Protocol struct {
	peer        *streammux.Peer
	manager     *Manager
	config      ProtocolConfig
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	attachments map[streammux.StreamID]*Attachment
	closeOnce   sync.Once
}

// RegisterProtocol atomically registers every configured handler on peer.
// peer and manager remain owned by their callers. The returned Protocol must
// be closed; peer shutdown also closes it automatically.
func RegisterProtocol(peer *streammux.Peer, manager *Manager, config ProtocolConfig) (*Protocol, error) {
	if peer == nil || manager == nil {
		return nil, errors.New("pty: nil Protocol dependency")
	}
	defaults := DefaultProtocolConfig()
	if config.Version == 0 {
		config.Version = defaults.Version
	}
	if config.MaxListItems == 0 {
		config.MaxListItems = defaults.MaxListItems
	}
	if config.MaxCommandBytes == 0 {
		config.MaxCommandBytes = defaults.MaxCommandBytes
	}
	if config.MaxDirectoryBytes == 0 {
		config.MaxDirectoryBytes = defaults.MaxDirectoryBytes
	}
	if config.MaxArgumentBytes == 0 {
		config.MaxArgumentBytes = defaults.MaxArgumentBytes
	}
	if config.MaxArguments == 0 {
		config.MaxArguments = defaults.MaxArguments
	}
	if config.MaxEnvironment == 0 {
		config.MaxEnvironment = defaults.MaxEnvironment
	}
	if config.MaxListItems < 1 || config.MaxCommandBytes < 1 || config.MaxDirectoryBytes < 1 || config.MaxArgumentBytes < 1 || config.MaxArguments < 1 || config.MaxEnvironment < 1 {
		return nil, errors.New("pty: invalid list limit")
	}
	if manager.config.ReadBufferBytes > peer.MaxFrameBytes() {
		return nil, errors.New("pty: ReadBufferBytes exceeds peer frame payload limit")
	}
	if err := config.Types.validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Protocol{peer: peer, manager: manager, config: config, ctx: ctx, cancel: cancel, attachments: make(map[streammux.StreamID]*Attachment)}
	handlers := map[streammux.MessageType]streammux.Handler{config.Types.Open: p.handleOpen, config.Types.List: p.handleList, config.Types.Inspect: p.handleInspect, config.Types.Attach: p.handleAttach, config.Types.Detach: p.handleDetach, config.Types.Input: p.handleInput, config.Types.Resize: p.handleResize, config.Types.Kill: p.handleKill}
	if err := peer.RegisterHandlers(handlers); err != nil {
		cancel()
		return nil, err
	}
	go func() {
		select {
		case <-peer.Done():
			_ = p.Close()
		case <-ctx.Done():
		}
	}()
	return p, nil
}

// Close is idempotent and detaches this connection's attachments without
// stopping their Sessions or closing the Peer.
func (p *Protocol) Close() error {
	p.closeOnce.Do(func() {
		p.cancel()
		p.mu.Lock()
		values := make([]*Attachment, 0, len(p.attachments))
		for _, a := range p.attachments {
			values = append(values, a)
		}
		clear(p.attachments)
		p.mu.Unlock()
		for _, a := range values {
			_ = a.Close()
		}
		for _, a := range values {
			<-a.Done()
		}
	})
	return nil
}

func (p *Protocol) handleOpen(ctx context.Context, peer *streammux.Peer, request streammux.Frame) error {
	if request.Header.StreamID != 0 {
		return p.respondError(ctx, request, CodeInvalidRequest, ErrInvalidStreamID)
	}
	var payload OpenRequest
	if err := p.decode(request, &payload); err != nil {
		return p.respondError(ctx, request, decodeErrorCode(err), err)
	}
	if err := p.validateOpenRequest(payload); err != nil {
		return p.respondError(ctx, request, CodeInvalidRequest, err)
	}
	spec := ProcessSpec{Command: payload.Command, Args: payload.Args, Env: payload.Env, Dir: payload.Dir, InitialSize: payload.InitialSize}
	if err := p.authorize(ctx, AuthorizationRequest{Operation: OperationOpen, ProcessSpec: &spec}); err != nil {
		return p.respondError(ctx, request, CodePermissionDenied, err)
	}
	session, err := p.manager.Open(ctx, spec)
	if err != nil {
		return p.respondOperationError(ctx, request, err)
	}
	info := session.Info()
	return p.respond(ctx, request, ProtocolResponse{Version: p.config.Version, Session: &info})
}
func (p *Protocol) handleList(ctx context.Context, peer *streammux.Peer, request streammux.Frame) error {
	if request.Header.StreamID != 0 {
		return p.respondError(ctx, request, CodeInvalidRequest, ErrInvalidStreamID)
	}
	var payload ListRequest
	if err := p.decode(request, &payload); err != nil {
		return p.respondError(ctx, request, decodeErrorCode(err), err)
	}
	if err := p.authorize(ctx, AuthorizationRequest{Operation: OperationList}); err != nil {
		return p.respondError(ctx, request, CodePermissionDenied, err)
	}
	limit := payload.Limit
	if limit == 0 {
		limit = p.config.MaxListItems
	}
	if limit < 1 || limit > p.config.MaxListItems {
		return p.respondError(ctx, request, CodeInvalidRequest, errors.New("invalid list limit"))
	}
	generation, all := p.manager.ListSnapshot()
	if p.config.FilterSessions != nil {
		filtered, err := p.config.FilterSessions(ctx, all)
		if err != nil {
			return p.respondError(ctx, request, CodePermissionDenied, err)
		}
		all = filtered
	}
	position := 0
	if payload.Cursor != "" {
		decoded, err := decodeCursor(payload.Cursor)
		if err != nil || decoded.generation != generation {
			return p.respondError(ctx, request, CodeInvalidRequest, errors.New("stale or invalid cursor"))
		}
		position = decoded.position
	}
	if position < 0 || position > len(all) {
		return p.respondError(ctx, request, CodeInvalidRequest, errors.New("invalid cursor position"))
	}
	end := position + limit
	if end > len(all) {
		end = len(all)
	}
	for end > position {
		candidateNext := ""
		if end < len(all) {
			candidateNext = encodeCursor(generation, end)
		}
		candidate, err := json.Marshal(ProtocolResponse{Version: p.config.Version, Sessions: all[position:end], NextCursor: candidateNext})
		if err != nil {
			return err
		}
		if len(candidate) <= p.peer.MaxFrameBytes() {
			break
		}
		end--
	}
	if end == position && position < len(all) {
		return p.respondError(ctx, request, CodeInvalidRequest, errors.New("one session entry exceeds frame payload limit"))
	}
	next := ""
	if end < len(all) {
		next = encodeCursor(generation, end)
	}
	return p.respond(ctx, request, ProtocolResponse{Version: p.config.Version, Sessions: all[position:end], NextCursor: next})
}

func (p *Protocol) validateOpenRequest(request OpenRequest) error {
	if len(request.Command) > p.config.MaxCommandBytes || len(request.Dir) > p.config.MaxDirectoryBytes {
		return errors.New("process specification string limit exceeded")
	}
	if len(request.Args) > p.config.MaxArguments || len(request.Env) > p.config.MaxEnvironment {
		return errors.New("process specification item limit exceeded")
	}
	values := append([]string{request.Command, request.Dir}, request.Args...)
	values = append(values, request.Env...)
	for _, value := range values {
		if len(value) > p.config.MaxArgumentBytes || strings.IndexByte(value, 0) >= 0 {
			return errors.New("invalid process specification string")
		}
	}
	return nil
}
func (p *Protocol) handleInspect(ctx context.Context, peer *streammux.Peer, request streammux.Frame) error {
	if len(request.Payload) != 0 {
		return p.respondError(ctx, request, CodeInvalidRequest, ErrInvalidPayload)
	}
	if err := p.authorize(ctx, AuthorizationRequest{Operation: OperationInspect, SessionID: request.Header.StreamID}); err != nil {
		return p.respondError(ctx, request, CodePermissionDenied, err)
	}
	session, ok := p.manager.Get(request.Header.StreamID)
	if !ok {
		return p.respondError(ctx, request, CodeSessionNotFound, ErrSessionNotFound)
	}
	info := session.Info()
	return p.respond(ctx, request, ProtocolResponse{Version: p.config.Version, Session: &info})
}
func (p *Protocol) handleAttach(ctx context.Context, peer *streammux.Peer, request streammux.Frame) error {
	var payload AttachRequest
	if err := p.decode(request, &payload); err != nil {
		return p.respondError(ctx, request, decodeErrorCode(err), err)
	}
	if err := p.authorize(ctx, AuthorizationRequest{Operation: OperationAttach, SessionID: request.Header.StreamID}); err != nil {
		return p.respondError(ctx, request, CodePermissionDenied, err)
	}
	session, ok := p.manager.Get(request.Header.StreamID)
	if !ok {
		return p.respondError(ctx, request, CodeSessionNotFound, ErrSessionNotFound)
	}
	p.mu.Lock()
	if _, exists := p.attachments[request.Header.StreamID]; exists {
		p.mu.Unlock()
		return p.respondError(ctx, request, CodeAlreadyAttached, ErrAlreadyAttached)
	}
	p.mu.Unlock()
	sink := &protocolOutputSink{protocol: p, streamID: request.Header.StreamID}
	attachment, result, err := session.Attach(p.ctx, AttachOptions{Replay: payload.Replay, ResumeAfter: payload.ResumeAfter, Paused: true}, sink)
	if err != nil {
		return p.respondOperationError(ctx, request, err)
	}
	sink.replayLast = result.ReplayLast
	p.mu.Lock()
	p.attachments[request.Header.StreamID] = attachment
	p.mu.Unlock()
	cleanup := func() {
		p.mu.Lock()
		if p.attachments[request.Header.StreamID] == attachment {
			delete(p.attachments, request.Header.StreamID)
		}
		p.mu.Unlock()
		_ = attachment.Close()
	}
	if err = p.respond(ctx, request, ProtocolResponse{Version: p.config.Version, Attach: &result}); err != nil {
		cleanup()
		return err
	}
	begin := ReplayMetadata{Version: p.config.Version, First: result.ReplayFirst, Last: result.ReplayLast, NextLive: result.NextLive, Truncated: result.Truncated}
	if err = p.emitJSON(ctx, p.config.Types.ReplayBegin, request.Header.StreamID, begin, streammux.TrafficControl); err != nil {
		cleanup()
		return err
	}
	if result.ReplayLast == 0 {
		if err = p.emitEmpty(ctx, p.config.Types.ReplayEnd, request.Header.StreamID, streammux.TrafficControl); err != nil {
			cleanup()
			return err
		}
		sink.replayEnded = true
	}
	if err = attachment.Activate(); err != nil {
		cleanup()
		return err
	}
	go p.watchAttachment(request.Header.StreamID, attachment)
	return nil
}

func (p *Protocol) watchAttachment(id streammux.StreamID, attachment *Attachment) {
	select {
	case <-attachment.Done():
		p.mu.Lock()
		if p.attachments[id] == attachment {
			delete(p.attachments, id)
		}
		p.mu.Unlock()
		reason := attachment.Reason()
		if err := attachment.Err(); err != nil && (reason == AttachmentQueueOverflow || reason == AttachmentSinkFailure || reason == AttachmentDrainTimeout) {
			code := CodeBackendFailure
			if errors.Is(err, ErrQueueOverflow) {
				code = CodeQueueOverflow
			}
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			_ = p.emitJSON(ctx, p.config.Types.Error, id, ProtocolErrorEvent{Version: p.config.Version, Code: code, Message: err.Error()}, streammux.TrafficControl)
			cancel()
			_ = p.peer.Close()
		}
	case <-p.ctx.Done():
	}
}
func (p *Protocol) handleDetach(ctx context.Context, peer *streammux.Peer, request streammux.Frame) error {
	if len(request.Payload) != 0 {
		return p.respondError(ctx, request, CodeInvalidRequest, ErrInvalidPayload)
	}
	if err := p.authorize(ctx, AuthorizationRequest{Operation: OperationDetach, SessionID: request.Header.StreamID}); err != nil {
		return p.respondError(ctx, request, CodePermissionDenied, err)
	}
	p.mu.Lock()
	a := p.attachments[request.Header.StreamID]
	delete(p.attachments, request.Header.StreamID)
	p.mu.Unlock()
	if a == nil {
		return p.respondError(ctx, request, CodeSessionNotFound, ErrAttachmentClosed)
	}
	_ = a.Close()
	return p.respond(ctx, request, ProtocolResponse{Version: p.config.Version})
}
func (p *Protocol) handleInput(ctx context.Context, peer *streammux.Peer, request streammux.Frame) error {
	if request.Header.Flags != streammux.FlagEvent {
		return ErrUnexpectedFrame
	}
	if err := p.authorize(ctx, AuthorizationRequest{Operation: OperationInput, SessionID: request.Header.StreamID}); err != nil {
		return err
	}
	session, ok := p.manager.Get(request.Header.StreamID)
	if !ok {
		return ErrSessionNotFound
	}
	return session.Input(ctx, request.Payload)
}
func (p *Protocol) handleResize(ctx context.Context, peer *streammux.Peer, request streammux.Frame) error {
	size, err := DecodeSize(request.Payload)
	if err != nil {
		return p.respondError(ctx, request, CodeInvalidRequest, err)
	}
	if err = p.authorize(ctx, AuthorizationRequest{Operation: OperationResize, SessionID: request.Header.StreamID}); err != nil {
		return p.respondError(ctx, request, CodePermissionDenied, err)
	}
	session, ok := p.manager.Get(request.Header.StreamID)
	if !ok {
		return p.respondError(ctx, request, CodeSessionNotFound, ErrSessionNotFound)
	}
	if err = session.Resize(ctx, size); err != nil {
		return p.respondOperationError(ctx, request, err)
	}
	return p.respond(ctx, request, ProtocolResponse{Version: p.config.Version})
}
func (p *Protocol) handleKill(ctx context.Context, peer *streammux.Peer, request streammux.Frame) error {
	if len(request.Payload) != 0 {
		return p.respondError(ctx, request, CodeInvalidRequest, ErrInvalidPayload)
	}
	if err := p.authorize(ctx, AuthorizationRequest{Operation: OperationKill, SessionID: request.Header.StreamID}); err != nil {
		return p.respondError(ctx, request, CodePermissionDenied, err)
	}
	if err := p.manager.Kill(ctx, request.Header.StreamID); err != nil {
		return p.respondOperationError(ctx, request, err)
	}
	return p.respond(ctx, request, ProtocolResponse{Version: p.config.Version})
}

func (p *Protocol) authorize(ctx context.Context, request AuthorizationRequest) error {
	if p.config.Authorize == nil {
		return nil
	}
	return p.config.Authorize(ctx, request)
}

type protocolOutputSink struct {
	protocol    *Protocol
	streamID    streammux.StreamID
	mu          sync.Mutex
	replayLast  uint64
	replayEnded bool
}

func (s *protocolOutputSink) Send(ctx context.Context, event AttachmentEvent) error {
	switch event.Kind {
	case AttachmentOutput:
		if event.Output == nil {
			return ErrInvalidPayload
		}
		frame, err := streammux.NewFrame(streammux.Header{Version: s.protocol.config.Version, MessageType: s.protocol.config.Types.Output, Flags: streammux.FlagEvent, StreamID: s.streamID}, event.Output.Data)
		if err != nil {
			return err
		}
		if err = s.protocol.peer.SendOwned(ctx, frame, streammux.TrafficBulk); err != nil {
			return err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.replayEnded && s.replayLast != 0 && event.Output.Sequence == s.replayLast {
			s.replayEnded = true
			return s.protocol.emitEmpty(ctx, s.protocol.config.Types.ReplayEnd, s.streamID, streammux.TrafficControl)
		}
		return nil
	case AttachmentExit:
		if event.Exit == nil {
			return ErrInvalidPayload
		}
		return s.protocol.emitJSON(ctx, s.protocol.config.Types.Exit, s.streamID, struct {
			Version uint16     `json:"version"`
			Exit    ExitStatus `json:"exit"`
		}{s.protocol.config.Version, *event.Exit}, streammux.TrafficControl)
	default:
		return ErrInvalidPayload
	}
}

func (p *Protocol) decode(frame streammux.Frame, target any) error {
	if frame.Header.Flags != streammux.FlagRequest {
		return ErrUnexpectedFrame
	}
	if len(frame.Payload) == 0 {
		return ErrInvalidPayload
	}
	if err := json.Unmarshal(frame.Payload, target); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	var version struct {
		Version uint16 `json:"version"`
	}
	if err := json.Unmarshal(frame.Payload, &version); err != nil || version.Version != p.config.Version {
		return ErrUnsupportedProtocolVersion
	}
	return nil
}

func decodeErrorCode(err error) ErrorCode {
	if errors.Is(err, ErrUnsupportedProtocolVersion) {
		return CodeUnsupportedVersion
	}
	return CodeInvalidRequest
}
func (p *Protocol) respond(ctx context.Context, request streammux.Frame, value ProtocolResponse) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	frame, err := streammux.NewOwnedFrame(streammux.Header{Version: p.config.Version, MessageType: request.Header.MessageType, Flags: streammux.FlagResponse, CorrelationID: request.Header.CorrelationID, StreamID: request.Header.StreamID}, payload)
	if err != nil {
		return err
	}
	return p.peer.Respond(ctx, request, frame)
}
func (p *Protocol) respondError(ctx context.Context, request streammux.Frame, code ErrorCode, cause error) error {
	return p.respond(ctx, request, ProtocolResponse{Version: p.config.Version, Error: &RemoteError{Code: code, Message: cause.Error()}})
}
func (p *Protocol) respondOperationError(ctx context.Context, request streammux.Frame, cause error) error {
	code := CodeBackendFailure
	switch {
	case errors.Is(cause, ErrSessionNotFound):
		code = CodeSessionNotFound
	case errors.Is(cause, ErrAttachmentLimit):
		code = CodeAttachmentLimit
	case errors.Is(cause, ErrSessionExited), errors.Is(cause, ErrSessionRunning):
		code = CodeSessionExited
	case errors.Is(cause, ErrManagerClosed):
		code = CodeManagerClosed
	case errors.Is(cause, ErrQueueOverflow):
		code = CodeQueueOverflow
	}
	return p.respondError(ctx, request, code, cause)
}
func (p *Protocol) emitJSON(ctx context.Context, messageType streammux.MessageType, id streammux.StreamID, value any, class streammux.TrafficClass) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	frame, err := streammux.NewOwnedFrame(streammux.Header{Version: p.config.Version, MessageType: messageType, Flags: streammux.FlagEvent, StreamID: id}, payload)
	if err != nil {
		return err
	}
	return p.peer.SendOwned(ctx, frame, class)
}

func (p *Protocol) emitEmpty(ctx context.Context, messageType streammux.MessageType, id streammux.StreamID, class streammux.TrafficClass) error {
	frame, err := streammux.NewFrame(streammux.Header{Version: p.config.Version, MessageType: messageType, Flags: streammux.FlagEvent, StreamID: id}, nil)
	if err != nil {
		return err
	}
	return p.peer.SendOwned(ctx, frame, class)
}

type listCursor struct {
	generation uint64
	position   int
}

func encodeCursor(generation uint64, position int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatUint(generation, 10) + ":" + strconv.Itoa(position)))
}
func decodeCursor(value string) (listCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return listCursor{}, err
	}
	parts := strings.Split(string(raw), ":")
	if len(parts) != 2 {
		return listCursor{}, errors.New("invalid cursor")
	}
	generation, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return listCursor{}, err
	}
	position, err := strconv.Atoi(parts[1])
	return listCursor{generation, position}, err
}
