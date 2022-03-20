package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testPath() string {
	return fmt.Sprintf("%v%cdb-path%c", os.TempDir(), os.PathSeparator, os.PathSeparator)
}

func beforeTest() {
	path := testPath()
	if _, err := os.Stat(path); err == nil {
		err := os.RemoveAll(path)
		if err != nil {
			log.Fatal(err)
		}
	}
	err := os.Mkdir(path, 0774)
	if err != nil {
		log.Fatal(err)
	}
}

func afterTest() {
	err := os.RemoveAll(testPath())
	if err != nil {
		log.Fatal(err)
	}
}

func TestNewManager(t *testing.T) {
	beforeTest()
	defer afterTest()
	a := assert.New(t)
	path := testPath()
	m := NewManager(path)
	a.NotNil(m)
	a.Equal(m.dbPath, path)
	a.Equal(m.counter, 0)
}

func TestManager_Init(t *testing.T) {
	beforeTest()
	defer afterTest()
	a := assert.New(t)
	path := testPath()
	m := NewManager(path)
	m.Init()
	defer m.Close()
	walDir := fmt.Sprintf("%vwal%c", testPath(), os.PathSeparator)
	_, err := os.Stat(walDir)
	a.ErrorIs(err, nil)

	a.Equal(1, m.counter)
	_, err = os.Stat(fmt.Sprintf("%vwal%c0", testPath(), os.PathSeparator))
	a.ErrorIs(err, nil)
}

func TestManager_Add(t *testing.T) {
	beforeTest()
	defer afterTest()
	a := assert.New(t)
	path := testPath()
	m := NewManager(path)
	m.Init()
	defer m.Close()
	rec := &Log{
		Key:   []byte{'k', 'e', 'y'},
		Value: []byte{'v', 'a', 'l', 'u', 'e'},
	}
	m.Add(rec)

	data, err := ioutil.ReadFile(fmt.Sprintf("%vwal%c0", path, os.PathSeparator))
	a.ErrorIs(err, nil)
	a.Equal(len(rec.Key)+len(rec.Value)+LogHeaderSize+BlockHeaderSize, len(data))
	crc := binary.LittleEndian.Uint32(data[0:4])
	size := binary.LittleEndian.Uint16(data[4:6])
	a.Equal(typeFull, data[6])
	expectedLen := uint16(len(rec.Key) + len(rec.Value) + 9)
	a.Equal(expectedLen, size)
	a.Equal(crc32.ChecksumIEEE(data[7:7+size]), crc)
}

func TestManager_RecoverLogs(t *testing.T) {
	beforeTest()
	defer afterTest()
	path := testPath()
	m := NewManager(path)
	m.Init()
	defer m.Close()
	a := assert.New(t)
	recCount := 1
	for i := 0; i < recCount; i++ {
		b := byte(i % 120)
		rec := &Log{
			Key:   bytes.Repeat([]byte{'k', b}, 10000),
			Value: bytes.Repeat([]byte{'v', b}, 10000),
		}
		m.Add(rec)
	}
	it, err := m.GetRecoverIterator()
	a.NotNil(it)
	logs := make([]*Log, 0)
	for it.Next() {
		l := it.RecoverCurrentFile()
		for i := 0; i < len(l); i++ {
			logs = append(logs, l[i])
		}
	}
	a.Nil(err)
	a.Equal(recCount, len(logs))

	for i := 0; i < recCount; i++ {
		b := byte(i % 120)
		rec := &Log{
			Key:   bytes.Repeat([]byte{'k', b}, 10000),
			Value: bytes.Repeat([]byte{'v', b}, 10000),
		}
		a.Equal(rec.Key, logs[i].Key)
		a.Equal(rec.Value, logs[i].Value)
	}

}

func BenchmarkManager_Add(b *testing.B) {
	beforeTest()
	defer afterTest()
	path := testPath()
	m := NewManager(path)
	m.Init()
	defer m.Close()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%v", i)
		rec := &Log{
			Key:   []byte(key),
			Value: []byte{'v', 'a', 'l', 'u', 'e'},
		}
		m.Add(rec)
	}
}
