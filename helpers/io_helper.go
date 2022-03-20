package helpers

import (
	"encoding/binary"
	"errors"
	"io"
)

func WriteUint32(w io.Writer, v uint32) error {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	_, err := w.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func WriteUint64(w io.Writer, v uint64) error {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	_, err := w.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func ReadUint32(r io.Reader) (uint32, error) {
	b := make([]byte, 4)
	n, err := io.ReadFull(r, b)
	if err != nil {
		return 0, err
	}
	if n != 4 {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func ReadUint64(r io.Reader) (uint64, error) {
	b := make([]byte, 8)
	n, err := io.ReadFull(r, b)
	if err != nil {
		return 0, err
	}
	if n != 8 {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func ReadSlice(rdr io.Reader) (*[]byte, error) {
	l, err := ReadUint32(rdr)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, l)
	rLen, err := io.ReadFull(rdr, buf)
	if err != nil {
		return nil, err
	}
	if rLen != int(l) {
		return nil, errors.New("slice length doesn't match with read data")
	}
	return &buf, nil
}
