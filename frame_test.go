package streammux_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/aruru-weed/streammux"
)

func TestFrameWireLayout(t *testing.T) {
	header := streammux.Header{Version: 1, MessageType: 7, Flags: streammux.FlagRequest, CorrelationID: 0x0102030405060708, StreamID: 0x1112131415161718}
	frame, err := streammux.NewFrame(header, []byte{0, 0xff, 'x'})
	if err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err = streammux.NewEncoder(&encoded, streammux.DefaultConfig()).WriteFrame(frame); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	if len(data) != streammux.HeaderSize+3 {
		t.Fatalf("wire length = %d", len(data))
	}
	if binary.BigEndian.Uint32(data[0:4]) != 3 || binary.BigEndian.Uint16(data[4:6]) != 1 || binary.BigEndian.Uint16(data[6:8]) != 7 {
		t.Fatalf("wire header = %x", data[:streammux.HeaderSize])
	}
	if binary.BigEndian.Uint64(data[12:20]) != uint64(header.CorrelationID) || binary.BigEndian.Uint64(data[20:28]) != uint64(header.StreamID) {
		t.Fatalf("wire IDs = %x", data[:streammux.HeaderSize])
	}
	decoded, err := streammux.NewDecoder(&encoded, streammux.DefaultConfig()).ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, frame) {
		t.Fatalf("decoded = %#v, want %#v", decoded, frame)
	}
}

func TestNewFrameOwnsCallerPayload(t *testing.T) {
	payload := []byte("oct")
	frame, err := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 1, Flags: streammux.FlagEvent, StreamID: 1}, payload)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'x'
	if string(frame.Payload) != "oct" {
		t.Fatalf("payload = %q", frame.Payload)
	}
}

func TestDecoderRejectsOversizedBeforePayloadRead(t *testing.T) {
	header := make([]byte, streammux.HeaderSize)
	binary.BigEndian.PutUint32(header[0:4], 1025)
	binary.BigEndian.PutUint16(header[4:6], 1)
	binary.BigEndian.PutUint16(header[6:8], 1)
	binary.BigEndian.PutUint32(header[8:12], uint32(streammux.FlagEvent))
	_, err := streammux.NewDecoder(bytes.NewReader(header), streammux.Config{MinVersion: 1, MaxVersion: 1, MaxFrameBytes: 1024}).ReadFrame()
	if !errors.Is(err, streammux.ErrFrameTooLarge) {
		t.Fatalf("error = %v", err)
	}
}

func TestDecoderRejectsTruncatedPayload(t *testing.T) {
	frame, _ := streammux.NewFrame(streammux.Header{Version: 1, MessageType: 1, Flags: streammux.FlagEvent, StreamID: 1}, []byte("abc"))
	var encoded bytes.Buffer
	_ = streammux.NewEncoder(&encoded, streammux.DefaultConfig()).WriteFrame(frame)
	data := encoded.Bytes()
	_, err := streammux.NewDecoder(bytes.NewReader(data[:len(data)-1]), streammux.DefaultConfig()).ReadFrame()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("error = %v", err)
	}
}

func TestApplicationValidator(t *testing.T) {
	want := errors.New("application rejected frame")
	config := streammux.DefaultConfig()
	config.Validate = func(frame streammux.Frame) error {
		if frame.Header.MessageType == 9 {
			return want
		}
		return nil
	}
	frame := streammux.Frame{Header: streammux.Header{PayloadLength: 0, Version: 1, MessageType: 9, Flags: streammux.FlagEvent, StreamID: 1}}
	if err := frame.Validate(config); !errors.Is(err, want) {
		t.Fatalf("Validate = %v", err)
	}
}
