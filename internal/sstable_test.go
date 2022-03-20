package internal

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testPath() string {
	return fmt.Sprintf("%v%cdb-path%c", os.TempDir(), os.PathSeparator, os.PathSeparator)
}

func beforeTest() {
	err := os.Mkdir(testPath(), 0774)
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

func TestSSTable_Save(t *testing.T) {
	beforeTest()
	defer afterTest()
	ss := NewSSTable(testPath(), "0.db")
	l := NewMemTable()
	l.Set([]byte("ca1"), []byte("test1"))
	l.Set([]byte("aa1"), []byte("test2"))
	l.Set([]byte("ab1"), []byte("test3"))
	err := ss.Save(l)
	ss.CloseFile()
	assert.Nil(t, err)
}

func TestSSTable_ReadFooter(t *testing.T) {
	beforeTest()
	defer afterTest()
	ss := NewSSTable(testPath(), "0.db")
	l := NewMemTable()
	l.Set([]byte("ca1"), []byte("test1"))
	l.Set([]byte("aa1"), []byte("test2"))
	l.Set([]byte("ab1"), []byte("test3"))
	err := ss.Save(l)
	assert.Nil(t, err)
	err = ss.ReadFooter()
	ss.CloseFile()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), ss.footerBlock.DataOffset)
	assert.NotEqual(t, uint64(0), ss.footerBlock.DataLength)
	assert.NotEqual(t, uint64(0), ss.footerBlock.IndexOffset)
	assert.NotEqual(t, uint64(0), ss.footerBlock.IndexLength)
	assert.NotEqual(t, uint64(0), ss.footerBlock.MetaOffset)
	assert.NotEqual(t, uint64(0), ss.footerBlock.MetaLength)
}

func TestSSTable_ReadMeta(t *testing.T) {
	beforeTest()
	defer afterTest()
	ss := NewSSTable(testPath(), "0.db")
	l := NewMemTable()
	l.Set([]byte("ca1"), []byte("test1"))
	l.Set([]byte("aa1"), []byte("test2"))
	l.Set([]byte("ab1"), []byte("test3"))
	err := ss.Save(l)
	assert.Nil(t, err)
	err = ss.ReadMeta()
	ss.CloseFile()
	assert.Nil(t, err)
	assert.Equal(t, []byte("aa1"), *ss.MinKey)
	assert.Equal(t, []byte("ca1"), *ss.MaxKey)
}

func TestSSTable_FindKeyInIndex(t *testing.T) {
	beforeTest()
	defer afterTest()
	ss := NewSSTable(testPath(), "0.db")
	l := NewMemTable()
	l.Set([]byte("ca1"), []byte("test1"))
	l.Set([]byte("aa1"), []byte("test2"))
	l.Set([]byte("ab1"), []byte("test3"))
	err := ss.Save(l)
	assert.Nil(t, err)

	pos, err := ss.FindKeyInIndex([]byte("ab1"))
	assert.Nil(t, err)
	assert.NotEqual(t, uint64(0), pos)

	_, err = ss.FindKeyInIndex([]byte("ac1"))
	assert.NotNil(t, err)
	assert.Equal(t, ErrIndexNotFound, err)
}

func TestSSTable_ReadValueAt(t *testing.T) {
	beforeTest()
	defer afterTest()
	ss := NewSSTable(testPath(), "0.db")
	l := NewMemTable()
	l.Set([]byte("ca1"), []byte("test1"))
	l.Set([]byte("aa1"), []byte("test2"))
	l.Set([]byte("ab1"), []byte("test3"))
	err := ss.Save(l)
	assert.Nil(t, err)

	tests := []struct {
		key  []byte
		want []byte
	}{
		{[]byte("ab1"), []byte("test3")},
		{[]byte("ca1"), []byte("test1")},
		{[]byte("aa1"), []byte("test2")},
	}

	for _, test := range tests {
		pos, err := ss.FindKeyInIndex(test.key)
		assert.Nil(t, err)
		value, err := ss.ReadValueAt(pos)
		assert.Nil(t, err)
		assert.Equal(t, test.want, value)
	}
}
