package wal

import (
	"bufio"
	"errors"
	"io"
	"log"
	"os"
)

type FileIterator struct {
	filePaths []string
	idx       int
	m         *Manager
}

func (f *FileIterator) Next() bool {
	if (f.idx + 1) >= len(f.filePaths) {
		return false
	}
	f.idx++
	return true
}

func (f *FileIterator) RecoverCurrentFile() []*Log {
	logs := make([]*Log, 0, 1024)
	file, err := os.Open(f.filePaths[f.idx])
	if err != nil {
		log.Println(err)
		return nil
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	walReader := NewWalReader(&WalOptions{BlockSize: BlockSize})
	for {
		l, err := walReader.ReadLog(reader)
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}
		logs = append(logs, l)
	}

	return logs
}

func (f *FileIterator) RemoveCurrentFile() error {
	if f.idx < len(f.filePaths) {
		return os.Remove(f.filePaths[f.idx])
	}
	return errors.New("index out of range for filePaths")
}
