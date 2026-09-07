// Package pty defines semantic PTY process operations and a streammux bridge.
package pty

import (
	"context"
	"errors"
	"io"
)

var (
	ErrCommandRequired     = errors.New("pty: command is required")
	ErrInvalidSize         = errors.New("pty: size must have positive columns and rows")
	ErrInvalidMessageTypes = errors.New("pty: message types must be non-zero and distinct")
	ErrSessionNotFound     = errors.New("pty: session not found")
	ErrUnexpectedFrame     = errors.New("pty: unexpected frame")
	ErrInvalidPayload      = errors.New("pty: invalid payload")
)

// Size is a terminal size in character columns and rows.
type Size struct {
	Cols int `json:"cols"`
	Rows int `json:"rows"`
}

// Validate rejects zero or negative terminal dimensions.
func (s Size) Validate() error {
	if s.Cols <= 0 || s.Rows <= 0 {
		return ErrInvalidSize
	}
	return nil
}

// ProcessSpec describes one PTY child. Args and Env are copied by Manager.Open.
// A nil Env inherits backend behavior; an empty non-nil Env remains empty.
type ProcessSpec struct {
	Command     string   `json:"command"`
	Args        []string `json:"args,omitempty"`
	Env         []string `json:"env,omitempty"`
	Dir         string   `json:"dir,omitempty"`
	InitialSize Size     `json:"initial_size"`
}

// Validate checks backend-independent process requirements.
func (s ProcessSpec) Validate() error {
	if s.Command == "" {
		return ErrCommandRequired
	}
	return nil
}

// Factory starts the tagged v0.1 Process API. The caller owns the returned
// Process and must eventually call Close.
type Factory interface {
	Start(context.Context, ProcessSpec) (Process, error)
}

// Process is the tagged v0.1 PTY lifecycle API. Output and Input remain valid
// until Close. Wait is safe to call repeatedly; Close releases resources.
type Process interface {
	Output() io.Reader
	Input() io.Writer
	Resize(Size) error
	Wait() error
	Kill() error
	Close() error
}

// ExitReason is platform-neutral process termination metadata.
type ExitReason string

const (
	ExitReasonExited      ExitReason = "exited"
	ExitReasonKilled      ExitReason = "killed"
	ExitReasonSignaled    ExitReason = "signaled"
	ExitReasonStartFailed ExitReason = "start_failed"
	ExitReasonIOFailure   ExitReason = "io_failure"
)

// ExitStatus is a platform-neutral, immutable process completion snapshot.
type ExitStatus struct {
	Reason ExitReason `json:"reason"`
	Code   int        `json:"code"`
	Signal string     `json:"signal,omitempty"`
	Error  string     `json:"error,omitempty"`
}

// ManagedFactory is the lifecycle-safe backend API used by Manager. It is
// separate from Factory so the tagged v0.1 Process API remains source
// compatible. StartManaged's context controls startup only; after success the
// caller owns the process.
type ManagedFactory interface {
	StartManaged(context.Context, ProcessSpec) (ManagedProcess, error)
}

// ManagedProcess separates process termination, waiting and PTY handle
// release. Methods may be called concurrently and Terminate, Kill, WaitStatus,
// and Close are idempotent. Close releases the PTY handle and does not itself
// signal a running process. WaitStatus is named distinctly because Go cannot
// overload the legacy Process.Wait method with a typed return value.
type ManagedProcess interface {
	Output() io.Reader
	Input() io.Writer
	Resize(Size) error
	Terminate() error
	Kill() error
	WaitStatus() (ExitStatus, error)
	Close() error
}
