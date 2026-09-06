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
	"sync"

	"github.com/aruru-weed/streammux/pty"
	creackpty "github.com/creack/pty"
	"golang.org/x/sys/unix"
)

var ErrSizeOverflow = errors.New("unixpty: terminal size exceeds uint16")

type Factory struct{}

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
