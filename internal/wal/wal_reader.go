package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

type WalReader struct {
	opts          *WalOptions
	offset        int
	lastBlockType byte
	// currentBlock *Block
}

func NewWalReader(opts *WalOptions) *WalReader {
	return &WalReader{
		opts: opts,
		// currentBlock: nil,
	}
}

/**
* Skips the trailer bytes if needed
 */
func (w *WalReader) skipIfNeeded(reader io.Reader) {
	o := w.offset % w.opts.BlockSize
	rem := w.opts.BlockSize - o
	if rem < BlockHeaderSize {
		// fmt.Println("skipping ", rem, " bytes")
		n, err := io.ReadFull(reader, make([]byte, rem))
		if err != nil {
			fmt.Println("error while skipping trailer: ", err)
		}
		w.offset += n
	}
}

func (w *WalReader) ReadLog(reader io.Reader) (*Log, error) {
	// fmt.Println("offset: ", w.offset)
	block, err := w.ReadBlock(reader)
	if err != nil {
		return nil, err
	}

	if block.Type != typeFirst && block.Type != typeFull {
		fmt.Println("unexpected block type at the start of log, skipping to valid block")
		for {
			var err error
			block, err = w.ReadBlock(reader)
			if err == io.EOF {
				return nil, err
			}
			if err != nil {
				fmt.Println("error while reading block: ", err)
				continue
			}
			if block.Type == typeFirst || block.Type == typeFull {
				break
			}
		}
	}

	if block.Type == typeFull {
		keyLen := binary.LittleEndian.Uint32(block.Payload[0:4])
		valLen := binary.LittleEndian.Uint32(block.Payload[4:8])
		l := Log{}

		l.Key = make([]byte, keyLen)
		copy(l.Key, block.Payload[LogHeaderSize:LogHeaderSize+keyLen])
		l.Value = make([]byte, valLen)
		copy(l.Value, block.Payload[LogHeaderSize+keyLen:LogHeaderSize+keyLen+valLen])
		block = nil
		return &l, nil
	} else if block.Type == typeFirst {

		payload := block.Payload
		if len(block.Payload) < LogHeaderSize {
			block, err = w.ReadBlock(reader)
			if err != nil {
				return nil, err
			}
			block.Size += uint16(len(payload))
			block.Payload = append(payload, block.Payload...)
		}

		keyLen := binary.LittleEndian.Uint32(block.Payload[0:4])
		valLen := binary.LittleEndian.Uint32(block.Payload[4:8])
		curIdx := uint32(LogHeaderSize)
		l := Log{}
		l.Key = make([]byte, keyLen)
		l.Value = make([]byte, valLen)
		keyIdx := uint32(0)
		valIdx := uint32(0)

		//read key first
		for keyIdx < keyLen {
			remaining := uint32(block.Size) - curIdx
			needed := keyLen - keyIdx
			am := int(min(needed, remaining))
			for i := 0; i < am; i++ {
				l.Key[keyIdx] = block.Payload[curIdx]
				keyIdx++
				curIdx++
			}
			var err error
			if curIdx == uint32(block.Size) {
				block, err = w.ReadBlock(reader)
				if err != nil {
					return nil, err
				}
				curIdx = 0
				if block.Type != typeMiddle && block.Type != typeLast {
					return nil, errors.New("expecting next block to be middle or last")
				}
			}
		}

		// read value
		for valIdx < valLen {
			remaining := uint32(block.Size) - curIdx
			needed := valLen - valIdx
			am := int(min(needed, remaining))
			for i := 0; i < am; i++ {
				l.Value[valIdx] = block.Payload[curIdx]
				valIdx++
				curIdx++
			}

			if block.Type == typeLast {
				// we're done
				block = nil
				return &l, nil
			}
			var err error
			if curIdx == uint32(block.Size) {
				block, err = w.ReadBlock(reader)
				if err != nil {
					return nil, err
				}
				curIdx = 0
				if block.Type != typeMiddle && block.Type != typeLast {
					return nil, errors.New("expecting next block to be middle or last")
				}
			}
		}

	}

	return nil, errors.New("unexpected error happened")
}

func (w *WalReader) ReadBlock(reader io.Reader) (*Block, error) {
	w.skipIfNeeded(reader)
	// read block header
	var header = make([]byte, BlockHeaderSize)
	n, err := io.ReadFull(reader, header)
	if err != nil {
		return nil, err
	}
	w.offset += n
	if n != BlockHeaderSize {
		return nil, errors.New("unexpected error while reading block header")
	}

	block := Block{}
	block.CRC = binary.LittleEndian.Uint32(header[0:4])
	block.Size = binary.LittleEndian.Uint16(header[4:6])
	block.Type = header[6]

	buf := make([]byte, block.Size)
	n, err = io.ReadFull(reader, buf)
	if err != nil {
		return nil, err
	}
	if n != int(block.Size) {
		return nil, fmt.Errorf("unexpected error while reading block, read %v expected %v", n, block.Size)
	}
	w.offset += n

	block.Payload = buf
	if crc32.ChecksumIEEE(block.Payload) != block.CRC {
		return nil, errors.New("crc32 doesn't match for the block")
	}
	w.lastBlockType = block.Type

	return &block, nil
}
