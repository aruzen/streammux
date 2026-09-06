package streammux

import (
	"context"
	"errors"
	"io"
	"sync"
)

var (
	ErrNilStream           = errors.New("streammux: nil stream")
	ErrInvalidVersionRange = errors.New("streammux: invalid version range")
	ErrConnClosed          = errors.New("streammux: connection closed")
)

type writeRequest struct {
	frame  Frame
	result chan error
}

// Conn owns one full-duplex stream. Send is concurrent-safe. ReadFrame must be
// called by at most one goroutine so the application remains the sole inbound
// dispatcher.
type Conn struct {
	stream  io.ReadWriteCloser
	decoder *Decoder
	encoder *Encoder
	config  Config
	ctx     context.Context
	cancel  context.CancelFunc
	writes  chan writeRequest
	done    chan struct{}

	closeOnce sync.Once
	mu        sync.Mutex
	err       error
}

func Open(parent context.Context, stream io.ReadWriteCloser, config Config) (*Conn, error) {
	if parent == nil {
		return nil, errors.New("streammux: nil context")
	}
	if stream == nil {
		return nil, ErrNilStream
	}
	config = config.withDefaults()
	if config.MinVersion > config.MaxVersion {
		return nil, ErrInvalidVersionRange
	}
	ctx, cancel := context.WithCancel(parent)
	conn := &Conn{
		stream: stream, decoder: NewDecoder(stream, config), encoder: NewEncoder(stream, config),
		config: config, ctx: ctx, cancel: cancel, writes: make(chan writeRequest, config.WriteQueue), done: make(chan struct{}),
	}
	go conn.writeLoop()
	go func() {
		<-ctx.Done()
		_ = stream.Close()
	}()
	return conn, nil
}

func (c *Conn) ReadFrame() (Frame, error) {
	frame, err := c.decoder.ReadFrame()
	if err != nil {
		c.setError(err)
		c.cancel()
	}
	return frame, err
}

func (c *Conn) Send(ctx context.Context, frame Frame) error {
	return c.send(ctx, frame.Clone())
}

// SendOwned transfers ownership of frame.Payload to Conn, even when this
// method returns an error. The caller must never mutate or reuse that storage.
func (c *Conn) SendOwned(ctx context.Context, frame Frame) error {
	return c.send(ctx, frame)
}

func (c *Conn) send(ctx context.Context, frame Frame) error {
	if ctx == nil {
		return errors.New("streammux: nil send context")
	}
	if err := frame.Validate(c.config); err != nil {
		return err
	}
	request := writeRequest{frame: frame, result: make(chan error, 1)}
	select {
	case c.writes <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.result()
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.result()
	}
}

func (c *Conn) writeLoop() {
	defer close(c.done)
	for {
		select {
		case <-c.ctx.Done():
			c.setError(c.ctx.Err())
			return
		case request := <-c.writes:
			err := c.encoder.WriteFrame(request.frame)
			request.result <- err
			if err != nil {
				c.setError(err)
				c.cancel()
				_ = c.stream.Close()
				return
			}
		}
	}
}

func (c *Conn) Done() <-chan struct{} { return c.done }

func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *Conn) setError(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.mu.Unlock()
}

func (c *Conn) result() error {
	if err := c.Err(); err != nil {
		return err
	}
	return ErrConnClosed
}

func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		streamErr := c.stream.Close()
		<-c.done
		if streamErr != nil && !errors.Is(streamErr, io.ErrClosedPipe) {
			c.setError(streamErr)
		}
	})
	err := c.Err()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
