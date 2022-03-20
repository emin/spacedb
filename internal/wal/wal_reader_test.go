package wal

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWalReader_ReadLog(t *testing.T) {
	testReadLog1(t)
	testReadLog2(t)
}

func testReadLog2(t *testing.T) {
	beforeTest()
	defer afterTest()
	path := fmt.Sprintf("%v%ctest.log", testPath(), os.PathSeparator)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0774)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	opts := &WalOptions{BlockSize: 100}

	recs := []Log{}

	for i := 0; i < 200; i++ {
		kLen := rand.Intn(100000)
		vLen := rand.Intn(100000)
		recs = append(recs, Log{
			Key:   bytes.Repeat([]byte{'k'}, kLen),
			Value: bytes.Repeat([]byte{'v'}, vLen),
		})
	}

	w := NewWalWriter(f, opts)
	for _, rec := range recs {
		_, err := w.WriteLog(&rec)
		w.Flush()
		assert.ErrorIs(t, nil, err)
	}
	f.Seek(0, 0)
	r := NewWalReader(opts)
	reader := bufio.NewReader(f)
	for _, rec := range recs {
		l, err := r.ReadLog(reader)
		assert.ErrorIs(t, err, nil)
		assert.Equal(t, len(rec.Key), len(l.Key))
		assert.Equal(t, len(rec.Value), len(l.Value))
		assert.Equal(t, rec.Key, l.Key)
		assert.Equal(t, rec.Value, l.Value)
	}
}

func testReadLog1(t *testing.T) {
	beforeTest()
	defer afterTest()
	path := fmt.Sprintf("%v%ctest.log", testPath(), os.PathSeparator)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0774)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	opts := &WalOptions{BlockSize: 100}

	recs := []Log{}

	for i := 0; i < 200; i++ {
		recs = append(recs, Log{
			Key:   bytes.Repeat([]byte{'k'}, 300),
			Value: bytes.Repeat([]byte{'v'}, 500),
		})
	}

	w := NewWalWriter(f, opts)
	for _, rec := range recs {
		_, err := w.WriteLog(&rec)
		w.Flush()
		assert.ErrorIs(t, nil, err)
	}
	f.Seek(0, 0)
	r := NewWalReader(opts)
	reader := bufio.NewReader(f)
	for _, rec := range recs {
		l, err := r.ReadLog(reader)
		assert.ErrorIs(t, err, nil)
		assert.Equal(t, len(rec.Key), len(l.Key))
		assert.Equal(t, len(rec.Value), len(l.Value))
		assert.Equal(t, rec.Key, l.Key)
		assert.Equal(t, rec.Value, l.Value)
	}
}

func TestWalReader_ReadBlock(t *testing.T) {
	testFullReadBlock(t)
	testFragmentedReadBlock(t)
}

func testFullReadBlock(t *testing.T) {
	data := []byte("hello")
	b := NewTestFile()
	blockSize := len(data) + BlockHeaderSize + 20
	opts := &WalOptions{BlockSize: blockSize}
	w := NewWalWriter(b, opts)
	_, err := w.Write(data)
	w.Flush()
	assert.ErrorIs(t, nil, err)
	r := NewWalReader(opts)
	expectedBlock := Block{
		Type:    typeFull,
		Payload: data,
		CRC:     crc32.ChecksumIEEE(data),
		Size:    uint16(len(data)),
	}
	block, err := r.ReadBlock(bufio.NewReader(b))
	assert.ErrorIs(t, nil, err)
	assert.Equal(t, expectedBlock, *block)
}

func testFragmentedReadBlock(t *testing.T) {

	data := []byte("hellohello")
	b := NewTestFile()
	blockSize := len(data)/2 + BlockHeaderSize
	opts := &WalOptions{BlockSize: blockSize}
	w := NewWalWriter(b, opts)
	_, err := w.Write(data)
	w.Flush()
	assert.ErrorIs(t, nil, err)
	r := NewWalReader(opts)
	expectedBlock := Block{
		Type:    typeFirst,
		Payload: data[:len(data)/2],
		CRC:     crc32.ChecksumIEEE(data[:len(data)/2]),
		Size:    uint16(len(data) / 2),
	}
	block, err := r.ReadBlock(bufio.NewReader(b))
	assert.ErrorIs(t, nil, err)
	assert.Equal(t, expectedBlock, *block)
}
