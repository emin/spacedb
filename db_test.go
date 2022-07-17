package spacedb

import (
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/emin/spacedb/internal"
	"github.com/emin/spacedb/internal/wal"
	"github.com/stretchr/testify/assert"
)

func testPath() string {
	return path.Join(os.TempDir(), "db-path")
}

func beforeTest() {
	if info, err := os.Stat(testPath()); err == nil && info.IsDir() {
		os.RemoveAll(testPath())
	}
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

func Test_loadSSTableMetaData(t *testing.T) {
	beforeTest()
	defer afterTest()
	dbPath := testPath()
	walManager := wal.NewManager(dbPath)
	db := &SpaceDBImpl{dbPath: dbPath,
		rwLock:          &sync.RWMutex{},
		walManager:      walManager,
		curMemTable:     internal.NewMemTable(),
		sstableMetadata: [][]*internal.MetaBlock{},
	}

	db.curMemTable.Set([]byte("1"), []byte("value1"))
	db.curMemTable.Set([]byte("2"), []byte("value2"))

	for j := 0; j < 4; j++ {
		for i := 0; i < j+1; i++ {
			fName := fmt.Sprintf("%d_%d.db", j, i)
			t := internal.NewSSTable(dbPath, fName)
			t.Save(db.curMemTable)
		}
	}

	db.loadSSTableMetaData()
	for j := 0; j < 4; j++ {
		assert.Equal(t, j+1, len(db.sstableMetadata[j]))
	}

}

func BenchmarkSet(b *testing.B) {
	beforeTest()
	defer afterTest()
	db := New(testPath())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("k%v", i)
		v := fmt.Sprintf("value = %v", i)
		db.Set([]byte(k), &DBValue{
			IsDeleted: false,
			Value:     []byte(v),
		})
	}
}
