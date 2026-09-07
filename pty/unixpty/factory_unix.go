//go:build darwin || linux

// Package unixpty implements pty.Factory on Unix systems supported by creack/pty.
package unixpty

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"

	"github.com/aruzen/streammux/pty"
	creackpty "github.com/creack/pty"
	"golang.org/x/sys/unix"
)

var (
	// ErrSizeOverflow indicates a terminal size unsupported by the Unix API.
	ErrSizeOverflow = errors.New("unixpty: terminal size exceeds uint16")
	// ErrInvalidProcessSpec indicates a NUL-containing exec parameter.
	ErrInvalidProcessSpec = errors.New("unixpty: process specification contains NUL")
)

// Factory implements the tagged v0.1 pty.Factory API.
type Factory struct{}

// Start starts a legacy Process whose lifetime is tied to ctx by exec.CommandContext.
func (Factory) Start(ctx context.Context, spec pty.ProcessSpec) (pty.Process, error) {
	if ctx == nil {
		return nil, errors.New("unixpty: nil context")
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	command := exec.CommandContext(ctx, spec.Command, spec.Args...)
	command.Env = spec.Env
	command.Dir = spec.Dir
	file, err := creackpty.Start(command)
	if err != nil {
		return nil, fmt.Errorf("unixpty: start: %w", err)
	}
	return &process{command: command, file: file, output: output{Reader: file}, done: make(chan struct{})}, nil
}

type process struct {
	command *exec.Cmd
	file    *os.File
	output  output
	done    chan struct{}

	waitOnce  sync.Once
	waitErr   error
	closeOnce sync.Once
	closeErr  error
}

func (p *process) Output() io.Reader { return p.output }
func (p *process) Input() io.Writer  { return p.file }

func (p *process) Resize(size pty.Size) error {
	if err := size.Validate(); err != nil {
		return err
	}
	if size.Cols > math.MaxUint16 || size.Rows > math.MaxUint16 {
		return ErrSizeOverflow
	}
	return creackpty.Setsize(p.file, &creackpty.Winsize{Cols: uint16(size.Cols), Rows: uint16(size.Rows)})
}

func (p *process) Wait() error {
	p.waitOnce.Do(func() {
		p.waitErr = p.command.Wait()
		close(p.done)
	})
	<-p.done
	return p.waitErr
}

// ExitCode exposes the completed process status to optional protocol layers.
// It returns -1 when the process has not produced a usable exit status.
func (p *process) ExitCode() int {
	_ = p.Wait()
	if p.command.ProcessState == nil {
		return -1
	}
	return p.command.ProcessState.ExitCode()
}

func (p *process) Kill() error {
	err := p.command.Process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}

func (p *process) Close() error {
	p.closeOnce.Do(func() {
		fileErr := p.file.Close()
		if errors.Is(fileErr, os.ErrClosed) {
			fileErr = nil
		}
		killErr := p.Kill()
		p.closeErr = errors.Join(fileErr, killErr)
		_ = p.Wait()
	})
	return p.closeErr
}

type output struct{ io.Reader }

func (o output) Read(buffer []byte) (int, error) {
	n, err := o.Reader.Read(buffer)
	if errors.Is(err, unix.EIO) {
		return n, io.EOF
	}
	return n, err
}

var _ pty.Factory = Factory{}
var _ pty.Process = (*process)(nil)

// ManagedFactory provides lifecycle-separated processes for pty.Manager.
// Factory remains the source-compatible v0.1 backend.
type ManagedFactory struct{}

// StartManaged starts a new process group with its initial terminal size. ctx
// controls startup only; the returned process is owned by its caller.
func (ManagedFactory) StartManaged(ctx context.Context, spec pty.ProcessSpec) (pty.ManagedProcess, error) {
	if ctx == nil {
		return nil, errors.New("unixpty: nil context")
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	if err := spec.InitialSize.Validate(); err != nil {
		return nil, err
	}
	if spec.InitialSize.Cols > math.MaxUint16 || spec.InitialSize.Rows > math.MaxUint16 {
		return nil, ErrSizeOverflow
	}
	values := append([]string{spec.Command, spec.Dir}, spec.Args...)
	values = append(values, spec.Env...)
	for _, value := range values {
		if strings.IndexByte(value, 0) >= 0 {
			return nil, ErrInvalidProcessSpec
		}
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	command := exec.Command(spec.Command, append([]string(nil), spec.Args...)...)
	if spec.Env != nil {
		command.Env = append([]string(nil), spec.Env...)
	}
	command.Dir = spec.Dir
	file, err := creackpty.StartWithSize(command, &creackpty.Winsize{Cols: uint16(spec.InitialSize.Cols), Rows: uint16(spec.InitialSize.Rows)})
	if err != nil {
		return nil, fmt.Errorf("unixpty: start: %w", err)
	}
	result := &managedProcess{command: command, file: file, output: output{Reader: file}, done: make(chan struct{}), pgid: command.Process.Pid, running: true}
	select {
	case <-ctx.Done():
		_ = result.Kill()
		_, _ = result.WaitStatus()
		_ = result.Close()
		return nil, ctx.Err()
	default:
		return result, nil
	}
}

type managedProcess struct {
	command *exec.Cmd
	file    *os.File
	output  output
	done    chan struct{}

	waitOnce   sync.Once
	waitStatus pty.ExitStatus
	waitErr    error
	closeOnce  sync.Once
	closeErr   error
	mu         sync.Mutex
	killed     bool
	pgid       int
	running    bool
}

func (p *managedProcess) Output() io.Reader { return p.output }
func (p *managedProcess) Input() io.Writer  { return p.file }

func (p *managedProcess) Resize(size pty.Size) error {
	if err := size.Validate(); err != nil {
		return err
	}
	if size.Cols > math.MaxUint16 || size.Rows > math.MaxUint16 {
		return ErrSizeOverflow
	}
	return creackpty.Setsize(p.file, &creackpty.Winsize{Cols: uint16(size.Cols), Rows: uint16(size.Rows)})
}

func (p *managedProcess) Terminate() error { return p.signalGroup(unix.SIGTERM, false) }
func (p *managedProcess) Kill() error      { return p.signalGroup(unix.SIGKILL, true) }

func (p *managedProcess) signalGroup(signal unix.Signal, killed bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.running {
		return nil
	}
	err := unix.Kill(-p.pgid, signal)
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	if err == nil && killed {
		p.killed = true
	}
	return err
}

func (p *managedProcess) WaitStatus() (pty.ExitStatus, error) {
	p.waitOnce.Do(func() {
		waitErr := p.command.Wait()
		status := pty.ExitStatus{Reason: pty.ExitReasonExited, Code: 0}
		p.mu.Lock()
		p.running = false
		killed := p.killed
		if waitErr != nil {
			status.Code = -1
			var exitErr *exec.ExitError
			if errors.As(waitErr, &exitErr) {
				status.Code = exitErr.ExitCode()
				if wait, ok := exitErr.Sys().(syscall.WaitStatus); ok && wait.Signaled() {
					status.Reason = pty.ExitReasonSignaled
					status.Signal = wait.Signal().String()
				}
				if killed {
					status.Reason = pty.ExitReasonKilled
				}
			} else {
				status.Reason = pty.ExitReasonIOFailure
				status.Error = waitErr.Error()
				p.waitErr = waitErr
			}
		}
		p.waitStatus = status
		close(p.done)
		p.mu.Unlock()
	})
	<-p.done
	return p.waitStatus, p.waitErr
}

func (p *managedProcess) Close() error {
	p.closeOnce.Do(func() {
		p.closeErr = p.file.Close()
		if errors.Is(p.closeErr, os.ErrClosed) {
			p.closeErr = nil
		}
	})
	return p.closeErr
}

var _ pty.ManagedFactory = ManagedFactory{}
var _ pty.ManagedProcess = (*managedProcess)(nil)
