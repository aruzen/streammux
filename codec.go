package streammux

import (
	"encoding/binary"
	"errors"
	"io"
)

type Validator func(Frame) error

type Config struct {
	MinVersion    uint16
	MaxVersion    uint16
	MaxFrameBytes int
	WriteQueue    int
	Validate      Validator
}

func DefaultConfig() Config {
	return Config{MinVersion: 1, MaxVersion: 1, MaxFrameBytes: DefaultMaxFrameBytes, WriteQueue: 64}
}

func (c Config) withDefaults() Config {
	if c.MinVersion == 0 {
		c.MinVersion = 1
	}
	if c.MaxVersion == 0 {
		c.MaxVersion = c.MinVersion
	}
	if c.MaxFrameBytes <= 0 {
		c.MaxFrameBytes = DefaultMaxFrameBytes
	}
	if c.WriteQueue < 0 {
		c.WriteQueue = 0
	}
	return c
}

type Decoder struct {
	reader io.Reader
	config Config
}

type Encoder struct {
	writer io.Writer
	config Config
}

func NewDecoder(reader io.Reader, config Config) *Decoder {
	return &Decoder{reader: reader, config: config.withDefaults()}
}

func NewEncoder(writer io.Writer, config Config) *Encoder {
	return &Encoder{writer: writer, config: config.withDefaults()}
}

func (d *Decoder) ReadFrame() (Frame, error) {
	headerBytes := make([]byte, HeaderSize)
	if _, err := io.ReadFull(d.reader, headerBytes); err != nil {
		return Frame{}, err
	}
	header := decodeHeader(headerBytes)
	if err := header.validate(d.config); err != nil {
		return Frame{}, err
	}
	payload := make([]byte, int(header.PayloadLength))
	if _, err := io.ReadFull(d.reader, payload); err != nil {
		if errors.Is(err, io.EOF) {
			return Frame{}, io.ErrUnexpectedEOF
		}
		return Frame{}, err
	}
	frame := Frame{Header: header, Payload: payload}
	if err := frame.Validate(d.config); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func (e *Encoder) WriteFrame(frame Frame) error {
	frame.Header.PayloadLength = uint32(len(frame.Payload))
	if err := frame.Validate(e.config); err != nil {
		return err
	}
	var header [HeaderSize]byte
	encodeHeader(frame.Header, header[:])
	if err := writeFull(e.writer, header[:]); err != nil {
		return err
	}
	return writeFull(e.writer, frame.Payload)
}

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

func encodeHeader(header Header, target []byte) {
	binary.BigEndian.PutUint32(target[0:4], header.PayloadLength)
	binary.BigEndian.PutUint16(target[4:6], header.Version)
	binary.BigEndian.PutUint16(target[6:8], uint16(header.MessageType))
	binary.BigEndian.PutUint32(target[8:12], uint32(header.Flags))
	binary.BigEndian.PutUint64(target[12:20], uint64(header.CorrelationID))
	binary.BigEndian.PutUint64(target[20:28], uint64(header.StreamID))
}

func decodeHeader(source []byte) Header {
	return Header{
		PayloadLength: binary.BigEndian.Uint32(source[0:4]),
		Version:       binary.BigEndian.Uint16(source[4:6]),
		MessageType:   MessageType(binary.BigEndian.Uint16(source[6:8])),
		Flags:         Flags(binary.BigEndian.Uint32(source[8:12])),
		CorrelationID: CorrelationID(binary.BigEndian.Uint64(source[12:20])),
		StreamID:      StreamID(binary.BigEndian.Uint64(source[20:28])),
	}
}
