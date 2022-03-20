package internal

import "github.com/emin/skiplist"

type MemTable interface {
	Set(key, value []byte)
	Get(key []byte) []byte
	Delete(key []byte) bool
	Iterator() Iterator
	KeyCount() int64
	RawSize() int64
}

type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
}

type iteratorImpl struct {
	iter skiplist.Iterator
}

type memtableImpl struct {
	rep *skiplist.SkipList
}

func NewMemTable() MemTable {
	return &memtableImpl{
		rep: skiplist.New(),
	}
}

func (m *memtableImpl) Set(key, value []byte) {
	m.rep.Set(key, value)
}

func (m *memtableImpl) Get(key []byte) []byte {
	return m.rep.Get(key)
}

func (m *memtableImpl) Delete(key []byte) bool {
	return m.rep.Delete(key)
}

func (m *memtableImpl) RawSize() int64 {
	return m.rep.RawSize()
}

func (m *memtableImpl) Iterator() Iterator {
	return &iteratorImpl{iter: m.rep.Iterator()}
}

func (m *memtableImpl) KeyCount() int64 {
	return m.rep.KeyCount()
}

func (m *iteratorImpl) Next() bool {
	return m.iter.Next()
}

func (m *iteratorImpl) Key() []byte {
	return m.iter.Key()
}

func (m *iteratorImpl) Value() []byte {
	return m.iter.Value()
}
