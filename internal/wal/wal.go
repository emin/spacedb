package wal

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

const MaxWalFileSize = 4 * 1024 * 1024
const BlockSize = 32 * 1024
const BlockHeaderSize = 7
const BlockPayloadSize = BlockSize - BlockHeaderSize
const flushOnWrite = true
const LogHeaderSize = 8

const typeFull uint8 = 1
const typeFirst uint8 = 2
const typeMiddle uint8 = 3
const typeLast uint8 = 4

// WAL blocks which will be stored into disk
type Block struct {
	CRC     uint32
	Size    uint16
	Type    uint8
	Payload []byte
}

// WAL Log data model
type Log struct {
	Key   []byte
	Value []byte
}

// Write-Ahead-Log Manager
// Provides interface for writing to and recovering from WAL files
// Its methods are *NOT* thread-safe
type Manager struct {
	writer          *WalWriter
	reader          *WalReader
	dbPath          string
	currentFile     *os.File
	counter         int
	currentFileSize int64
	opts            *WalOptions
}

// Return new WAL Manager
func NewManager(dbPath string) *Manager {
	opts := &WalOptions{
		BlockSize: BlockSize,
	}
	m := &Manager{
		dbPath: dbPath,
		reader: NewWalReader(opts),
		opts:   opts,
	}
	return m
}

// This should be called first for initialization.
// this will create wal/ directory if it does not exist
// under the dbPath, in WAL directory, a new WAL file will be created
func (m *Manager) Init() {
	walDir := path.Join(m.dbPath, "wal") //fmt.Sprintf("%v/wal/", m.dbPath)
	if _, err := os.Stat(walDir); err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(walDir, 0774)
			if err != nil {
				log.Println(err)
				return
			}
		} else {
			log.Fatal(err)
		}
	}
	m.createNewFile()
}

// Creates a new WAL file and maintain counter for WAL files.
// After this call newly created file will be used for logs
func (m *Manager) createNewFile() {
	p := path.Join(m.dbPath, "wal", fmt.Sprintf("%v.log", m.counter))
	for {
		_, err := os.Stat(p)
		if os.IsNotExist(err) {
			break
		}
		m.counter++
		p = path.Join(m.dbPath, "wal", fmt.Sprintf("%v.log", m.counter))
	}

	f, err := os.Create(p)
	if err != nil {
		log.Fatal(err)
	}
	m.currentFileSize = 0
	m.counter++
	if m.counter >= (1<<31 - 1) {
		m.counter = 0
	}
	m.currentFile = f
	m.writer = NewWalWriter(f, m.opts)
}

// Adds a log to WAL file
func (m *Manager) Add(l *Log) error {

	n, err := m.writer.WriteLog(l)
	if err != nil {
		return err
	}
	m.currentFileSize += int64(n)

	// if m.currentFileSize >= MaxWalFileSize {
	// 	m.SwitchFile()
	// }

	return nil
}

func (m *Manager) GetCurrentWalPath() string {
	currentPath := path.Join(m.dbPath, "wal", "current")
	data, err := ioutil.ReadFile(currentPath)
	if err != nil {
		log.Println(err)
		return ""
	}
	return string(data)
}

// Returns a FileIterator which can be used to iterate WAL files
// to recover the data
func (m *Manager) GetRecoverIterator() (*FileIterator, error) {
	dir := path.Join(m.dbPath, "wal")
	if _, err := os.Stat(dir); err != nil {
		return nil, nil
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	sort.Slice(files, func(a, b int) bool {
		fA := files[a].Name()
		fB := files[b].Name()
		fA = strings.TrimSuffix(fA, ".log")
		fB = strings.TrimSuffix(fB, ".log")

		f1, err := strconv.ParseInt(fA, 10, 64)
		if err != nil {
			log.Println(err)
			return false
		}
		f2, err := strconv.ParseInt(fB, 10, 64)
		if err != nil {
			log.Println(err)
			return false
		}
		return f1 < f2
	})
	it := &FileIterator{
		filePaths: make([]string, 0, len(files)),
		idx:       -1,
		m:         m,
	}
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".log") {
			fPath := path.Join(dir, f.Name())
			fPathNew := fPath + ".old"
			err := os.Rename(fPath, fPathNew)
			if err != nil {
				log.Fatalf("Failed to rename file %v", fPath)
			}
			it.filePaths = append(it.filePaths, fPathNew)
		}
	}
	return it, nil
}

func min(x, y uint32) uint32 {
	if x < y {
		return x
	} else {
		return y
	}
}

// Gracefully closes WAL Manager
// it'll flush and close open files
// after calling this WAL Manager will be unusable
func (m *Manager) Close() {
	if m.writer != nil {
		err := m.writer.Flush()
		if err != nil {
			log.Println(err)
		}
	}
	m.writer = nil

	if m.currentFile != nil {
		err := m.currentFile.Close()
		if err != nil {
			log.Println(err)
		}
	}
	m.currentFile = nil
}

// Closes current file and creates new WAL file
// returns last wal file number
func (m *Manager) SwitchFile() string {
	name := m.currentFile.Name()
	m.Close()
	m.createNewFile()
	return name
}
