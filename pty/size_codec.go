package pty

import (
	"encoding/binary"
	"math"
)

// EncodeSize serializes size as two network-byte-order uint32 values.
func EncodeSize(size Size) ([]byte, error) {
	if err := size.Validate(); err != nil {
		return nil, err
	}
	if uint64(size.Cols) > uint64(math.MaxUint32) || uint64(size.Rows) > uint64(math.MaxUint32) {
		return nil, ErrInvalidSize
	}
	payload := make([]byte, 8)
	binary.BigEndian.PutUint32(payload[0:4], uint32(size.Cols))
	binary.BigEndian.PutUint32(payload[4:8], uint32(size.Rows))
	return payload, nil
}

// DecodeSize decodes two network-byte-order uint32 values into a validated Size.
func DecodeSize(payload []byte) (Size, error) {
	if len(payload) != 8 {
		return Size{}, ErrInvalidPayload
	}
	cols := binary.BigEndian.Uint32(payload[0:4])
	rows := binary.BigEndian.Uint32(payload[4:8])
	if uint64(cols) > uint64(math.MaxInt) || uint64(rows) > uint64(math.MaxInt) {
		return Size{}, ErrInvalidSize
	}
	size := Size{Cols: int(cols), Rows: int(rows)}
	if err := size.Validate(); err != nil {
		return Size{}, err
	}
	return size, nil
}
