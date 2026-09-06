// Package pty defines semantic PTY process operations and a streammux bridge.
package pty

import (
	"context"
	"errors"
	"io"
)

var (
	ErrCommandRequired = errors.New("pty: command is required")
	ErrInvalidSize     = errors.New("pty: size must have positive columns and rows")
)

type Size struct {
	Cols int
	Rows int
}

func (s Size) Validate() error {
	if s.Cols <= 0 || s.Rows <= 0 {
		return ErrInvalidSize
	}
	return nil
}

type ProcessSpec struct {
	Command string
	Args    []string
	Env     []string
	Dir     string
}

func (s ProcessSpec) Validate() error {
	if s.Command == "" {
		return ErrCommandRequired
	}
	return nil
}

type Factory interface {
	Start(context.Context, ProcessSpec) (Process, error)
}

type Process interface {
	Output() io.Reader
	Input() io.Writer
	Resize(Size) error
	Wait() error
	Kill() error
	Close() error
}
