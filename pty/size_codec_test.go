package pty_test

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/aruzen/streammux/pty"
)

func TestSizeCodec(t *testing.T) {
	want := pty.Size{Cols: 120, Rows: 40}
	payload, err := pty.EncodeSize(want)
	if err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(payload[:4]) != 120 || binary.BigEndian.Uint32(payload[4:]) != 40 {
		t.Fatalf("EncodeSize = %x", payload)
	}
	got, err := pty.DecodeSize(payload)
	if err != nil || got != want {
		t.Fatalf("DecodeSize = %#v, %v", got, err)
	}
}

func TestSizeCodecRejectsInvalidPayload(t *testing.T) {
	if _, err := pty.EncodeSize(pty.Size{}); !errors.Is(err, pty.ErrInvalidSize) {
		t.Fatalf("EncodeSize error = %v", err)
	}
	if _, err := pty.DecodeSize(make([]byte, 7)); !errors.Is(err, pty.ErrInvalidPayload) {
		t.Fatalf("DecodeSize error = %v", err)
	}
}
