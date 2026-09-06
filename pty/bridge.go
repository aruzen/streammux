package pty

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aruzen/streammux"
)

var (
	ErrNilProcess       = errors.New("pty: nil process")
	ErrNilSender        = errors.New("pty: nil frame sender")
	ErrInvalidStreamID  = errors.New("pty: invalid StreamID")
	ErrInvalidInputType = errors.New("pty: unexpected input MessageType")
)

const DefaultReadBufferSize = 32 * 1024

type OwnedSender interface {
	SendOwned(context.Context, streammux.Frame) error
}

type BridgeConfig struct {
	Version        uint16
	StreamID       streammux.StreamID
	InputType      streammux.MessageType
	OutputType     streammux.MessageType
	ReadBufferSize int
}

type Bridge struct {
	process Process
	sender  OwnedSender
	config  BridgeConfig
}

func NewBridge(process Process, sender OwnedSender, config BridgeConfig) (*Bridge, error) {
	if process == nil {
		return nil, ErrNilProcess
	}
	if sender == nil {
		return nil, ErrNilSender
	}
	if config.Version == 0 {
		config.Version = 1
	}
	if config.StreamID == 0 {
		return nil, ErrInvalidStreamID
	}
	if config.InputType == 0 || config.OutputType == 0 {
		return nil, streammux.ErrInvalidMessageType
	}
	if config.ReadBufferSize <= 0 {
		config.ReadBufferSize = DefaultReadBufferSize
	}
	return &Bridge{process: process, sender: sender, config: config}, nil
}

// RunOutput forwards opaque PTY output. Cancellation closes the owned PTY
// process so a blocked read is interrupted.
func (b *Bridge) RunOutput(ctx context.Context) error {
	if ctx == nil {
		return errors.New("pty: nil context")
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = b.process.Close()
		case <-done:
		}
	}()
	buffer := make([]byte, b.config.ReadBufferSize)
	for {
		n, err := b.process.Output().Read(buffer)
		if n < 0 || n > len(buffer) {
			return fmt.Errorf("pty: invalid read count %d", n)
		}
		if n > 0 {
			owned := append([]byte(nil), buffer[:n]...)
			frame, frameErr := streammux.NewOwnedFrame(streammux.Header{
				Version: b.config.Version, MessageType: b.config.OutputType,
				Flags: streammux.FlagEvent, StreamID: b.config.StreamID,
			}, owned)
			if frameErr != nil {
				return frameErr
			}
			if sendErr := b.sender.SendOwned(ctx, frame); sendErr != nil {
				return sendErr
			}
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

func (b *Bridge) HandleInput(frame streammux.Frame) error {
	if frame.Header.StreamID != b.config.StreamID {
		return ErrInvalidStreamID
	}
	if frame.Header.MessageType != b.config.InputType || frame.Header.Flags != streammux.FlagEvent {
		return ErrInvalidInputType
	}
	return writeFull(b.process.Input(), frame.Payload)
}

func (b *Bridge) Resize(size Size) error { return b.process.Resize(size) }
func (b *Bridge) Wait() error            { return b.process.Wait() }
func (b *Bridge) Kill() error            { return b.process.Kill() }
func (b *Bridge) Close() error           { return b.process.Close() }

func writeFull(writer io.Writer, data []byte) error {
	for written := 0; written < len(data); {
		n, err := writer.Write(data[written:])
		if n < 0 || n > len(data)-written {
			return io.ErrShortWrite
		}
		written += n
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
