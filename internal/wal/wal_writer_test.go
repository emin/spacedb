package wal

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestFile struct {
	buf *bytes.Buffer
	rw  *bufio.ReadWriter
}

func NewTestFile() *TestFile {
	b := &bytes.Buffer{}
	return &TestFile{
		buf: b,
		rw:  bufio.NewReadWriter(bufio.NewReader(b), bufio.NewWriter(b)),
	}
}

func (f *TestFile) Write(p []byte) (int, error) {
	return f.buf.Write(p)
}

func (f *TestFile) Read(p []byte) (int, error) {
	return f.rw.Read(p)
}

func (f *TestFile) Close() error {
	return f.rw.Flush()
}

func TestWalWriter_Write(t *testing.T) {
	testFullType(t)
	testFragmented(t)
	testTrailer(t)
	testTrailer2(t)
}

func testFullType(t *testing.T) {
	data := []byte("hello")
	b := NewTestFile()
	w := NewWalWriter(b, &WalOptions{BlockSize: len(data) + BlockHeaderSize})
	n, err := w.Write(data)
	assert.ErrorIs(t, nil, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, typeFull, b.buf.Bytes()[6])
	assert.Equal(t, data, b.buf.Bytes()[BlockHeaderSize:])
}

func testFragmented(t *testing.T) {
	data := []byte("walwalwalm")
	b := NewTestFile()
	w := NewWalWriter(b, &WalOptions{BlockSize: len(data)/2 + BlockHeaderSize})
	n, err := w.Write(data)
	assert.ErrorIs(t, nil, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, typeFirst, b.buf.Bytes()[BlockHeaderSize-1])
	assert.EqualValues(t, data[:len(data)/2], b.buf.Bytes()[BlockHeaderSize:BlockHeaderSize+len(data)/2])
	assert.Equal(t, typeLast, b.buf.Bytes()[w.opts.BlockSize+BlockHeaderSize-1])
	assert.Equal(t, data[len(data)/2:], b.buf.Bytes()[w.opts.BlockSize+BlockHeaderSize:])
}

func testTrailer(t *testing.T) {
	data := []byte("12345678901234567890")
	trailerLen := 5
	b := NewTestFile()
	w := NewWalWriter(b, &WalOptions{BlockSize: len(data)/2 + BlockHeaderSize + trailerLen})
	n, err := w.Write(data)
	assert.ErrorIs(t, nil, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, typeFirst, b.buf.Bytes()[BlockHeaderSize-1])
	assert.Equal(t, data[:len(data)/2], b.buf.Bytes()[BlockHeaderSize:BlockHeaderSize+len(data)/2])
	assert.Equal(t, typeLast, b.buf.Bytes()[w.opts.BlockSize+BlockHeaderSize-1])
	assert.Equal(t, data[len(data)/2+trailerLen:], b.buf.Bytes()[w.opts.BlockSize+BlockHeaderSize:w.opts.BlockSize+BlockHeaderSize+len(data)/2-trailerLen])
}

func testTrailer2(t *testing.T) {
	data := []byte("1234567890123456789012345678901234567890")
	trailerLen := 5
	b := NewTestFile()
	w := NewWalWriter(b, &WalOptions{BlockSize: len(data)/3 + BlockHeaderSize + trailerLen})
	n, err := w.Write(data)
	assert.ErrorIs(t, nil, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, typeFirst, b.buf.Bytes()[BlockHeaderSize-1])
	assert.Equal(t, data[:len(data)/3], b.buf.Bytes()[BlockHeaderSize:BlockHeaderSize+len(data)/3])
	assert.Equal(t, typeMiddle, b.buf.Bytes()[w.opts.BlockSize+BlockHeaderSize-1])
	assert.Equal(t, typeLast, b.buf.Bytes()[w.opts.BlockSize*2+BlockHeaderSize-1])
}
