// Package streammux multiplexes opaque byte streams and typed control frames
// over one full-duplex byte stream.
package streammux

import (
	"errors"
	"fmt"
	"math"
)

const (
	HeaderSize           = 28
	DefaultMaxFrameBytes = 4 * 1024 * 1024
)

type MessageType uint16
type Flags uint32
type CorrelationID uint64
type StreamID uint64

const (
	FlagRequest Flags = 1 << iota
	FlagResponse
	FlagEvent
)

const knownFlags = FlagRequest | FlagResponse | FlagEvent

var (
	ErrFrameTooLarge      = errors.New("streammux: frame exceeds maximum size")
	ErrInvalidFrameLength = errors.New("streammux: invalid frame length")
	ErrUnsupportedVersion = errors.New("streammux: unsupported protocol version")
	ErrInvalidMessageType = errors.New("streammux: invalid message type")
	ErrInvalidFlags       = errors.New("streammux: invalid flags")
	ErrInvalidCorrelation = errors.New("streammux: invalid correlation ID")
)

type Header struct {
	PayloadLength uint32
	Version       uint16
	MessageType   MessageType
	Flags         Flags
	CorrelationID CorrelationID
	StreamID      StreamID
}

type Frame struct {
	Header  Header
	Payload []byte
}

func NewFrame(header Header, payload []byte) (Frame, error) {
	return newFrame(header, append([]byte(nil), payload...))
}

// NewOwnedFrame transfers payload ownership to the returned Frame. The caller
// must never mutate payload after this call, including after a send error.
func NewOwnedFrame(header Header, payload []byte) (Frame, error) {
	return newFrame(header, payload)
}

func newFrame(header Header, payload []byte) (Frame, error) {
	if uint64(len(payload)) > math.MaxUint32 {
		return Frame{}, ErrFrameTooLarge
	}
	header.PayloadLength = uint32(len(payload))
	frame := Frame{Header: header, Payload: payload}
	if err := frame.Validate(lenLimit(payload)); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func (f Frame) Clone() Frame {
	f.Payload = append([]byte(nil), f.Payload...)
	return f
}

func (f Frame) Validate(config Config) error {
	config = config.withDefaults()
	if len(f.Payload) > config.MaxFrameBytes || uint64(len(f.Payload)) > math.MaxUint32 {
		return fmt.Errorf("%w: %d", ErrFrameTooLarge, len(f.Payload))
	}
	if err := f.Header.validate(config); err != nil {
		return err
	}
	if uint32(len(f.Payload)) != f.Header.PayloadLength {
		return fmt.Errorf("%w: header=%d payload=%d", ErrInvalidFrameLength, f.Header.PayloadLength, len(f.Payload))
	}
	if config.Validate != nil {
		if err := config.Validate(f); err != nil {
			return err
		}
	}
	return nil
}

func (h Header) validate(config Config) error {
	if uint64(h.PayloadLength) > uint64(config.MaxFrameBytes) {
		return fmt.Errorf("%w: %d", ErrFrameTooLarge, h.PayloadLength)
	}
	if h.Version < config.MinVersion || h.Version > config.MaxVersion {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, h.Version)
	}
	if h.MessageType == 0 {
		return ErrInvalidMessageType
	}
	if h.Flags&^knownFlags != 0 || classCount(h.Flags) != 1 {
		return fmt.Errorf("%w: %#x", ErrInvalidFlags, h.Flags)
	}
	if h.Flags&(FlagRequest|FlagResponse) != 0 && h.CorrelationID == 0 {
		return ErrInvalidCorrelation
	}
	if h.Flags == FlagEvent && h.CorrelationID != 0 {
		return ErrInvalidCorrelation
	}
	return nil
}

func classCount(flags Flags) int {
	count := 0
	for _, flag := range []Flags{FlagRequest, FlagResponse, FlagEvent} {
		if flags&flag != 0 {
			count++
		}
	}
	return count
}

func lenLimit(payload []byte) Config {
	limit := len(payload)
	if limit == 0 {
		limit = 1
	}
	return Config{MinVersion: 1, MaxVersion: math.MaxUint16, MaxFrameBytes: limit}
}
