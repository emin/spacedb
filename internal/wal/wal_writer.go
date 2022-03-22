package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// header size of trailer
var trailer = []byte{0xfa, 0xfa, 0xfa, 0xfa, 0xfa, 0xfa, 0xfa}

type WalOptions struct {
	BlockSize int // Size of each WAL block
}

type WalWriter struct {
	f      *bufio.Writer
	offset int
	total  int
	opts   *WalOptions
}

func NewWalWriter(file io.Writer, opts *WalOptions) *WalWriter {
	return &WalWriter{
		f:      bufio.NewWriter(file),
		offset: 0,
		total:  0,
		opts:   opts,
	}
}

func (w *WalWriter) WriteLog(l *Log) (int, error) {
	keyLen := len(l.Key)
	valLen := len(l.Value)

	// 4byte length for each key and value
	// data layout: key length (4 byte) | value length (4 byte) | log type (1 byte)  | key | value
	totalLen := keyLen + valLen + LogHeaderSize
	kIdx := 0
	vIdx := 0

	allData := make([]byte, totalLen)

	c := 0

	binary.LittleEndian.PutUint32(allData[0:], uint32(keyLen))
	binary.LittleEndian.PutUint32(allData[4:], uint32(valLen))
	c = 8

	for ; kIdx < keyLen && c < totalLen; kIdx++ {
		allData[c] = l.Key[kIdx]
		c++
	}
	for ; vIdx < valLen && c < totalLen; vIdx++ {
		allData[c] = l.Value[vIdx]
		c++
	}
	n, err := w.Write(allData)
	if err != nil {
		return n, err
	}

	return n, err
}

func (w *WalWriter) Write(b []byte) (int, error) {

	offset := 0
	rem := len(b)

	for rem > 0 {
		leftOver := w.opts.BlockSize - w.offset
		if leftOver < BlockHeaderSize {
			if leftOver > 0 {
				w.f.Write(trailer[:leftOver])
			}
			w.total += w.offset + leftOver
			w.offset = 0
		}
		offset = len(b) - rem
		avail := w.opts.BlockSize - BlockHeaderSize - w.offset
		amount := avail
		if rem < avail {
			amount = rem
		}

		blockType := typeMiddle
		if offset == 0 && amount == rem {
			blockType = typeFull
		} else if offset == 0 {
			blockType = typeFirst
		} else if amount == rem {
			blockType = typeLast
		}

		payload := b[offset : offset+amount]
		chkSum := crc32.ChecksumIEEE(payload)
		header := make([]byte, BlockHeaderSize)
		binary.LittleEndian.PutUint32(header[0:], chkSum)
		binary.LittleEndian.PutUint16(header[4:], uint16(amount))
		header[6] = blockType

		n, err := w.f.Write(header)
		if err != nil && n != BlockHeaderSize {
			w.offset += n
			fmt.Println("error writing header: ", err)
			return offset, err
		}
		w.offset += n

		n, err = w.f.Write(payload)
		if err != nil && n != amount {
			w.offset += n
			fmt.Println("error writing payload: ", err)
			return offset, err
		}
		w.offset += n
		rem -= n
	}
	if flushOnWrite {
		w.f.Flush()
	}
	return len(b), nil
}

func (w *WalWriter) Flush() error {
	return w.f.Flush()
}
