package spacedb

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/emin/spacedb/internal"
	"github.com/emin/spacedb/internal/wal"
)

const MaxMemTableSize int64 = 4 * 1024 * 1024 // 4MB

type DBValue struct {
	IsDeleted bool
	Value     []byte
}

func (d *DBValue) Serialize() []byte {
	flag := byte(0)
	if d.IsDeleted {
		flag = byte(1)
	}
	return append([]byte{flag}, d.Value...)
}

func Deserialize(d []byte) *DBValue {
	return &DBValue{
		IsDeleted: d[0] == 1,
		Value:     d[1:],
	}
}

type SpaceDB interface {
	Set(key []byte, value *DBValue) error
	Get(key []byte) *DBValue
	Delete(key []byte) error
	KeyCount() int64
	Close()
}

type SpaceDBImpl struct {
	dbPath          string
	rwLock          *sync.RWMutex
	walManager      *wal.Manager
	curMemTable     internal.MemTable
	sstableMetadata [][]*internal.MetaBlock
	curFileNum      int
}

func New(dbPath string) SpaceDB {
	walManager := wal.NewManager(dbPath)
	db := &SpaceDBImpl{dbPath: dbPath,
		rwLock:          &sync.RWMutex{},
		walManager:      walManager,
		curMemTable:     internal.NewMemTable(),
		sstableMetadata: [][]*internal.MetaBlock{},
	}

	it, err := db.walManager.GetRecoverIterator()
	if err != nil {
		log.Fatalf("error while recovering from wal: %v", err)
	}

	db.walManager.Init()

	// recover from wal
	if it != nil {
		for it.Next() {
			logs := it.RecoverCurrentFile()
			for _, l := range logs {
				db.Set(l.Key, Deserialize(l.Value))
			}
			err := it.RemoveCurrentFile()
			if err != nil {
				log.Printf("error while removing wal file: %v\n", err)
			}
		}
	}

	// read sstable metadata
	err = db.loadSSTableMetaData()
	if err != nil {
		log.Printf("error while loading metadata: %v\n", err)
	}

	return db
}

func (g *SpaceDBImpl) loadSSTableMetaData() error {
	if _, err := os.Stat(g.dbPath); err != nil {
		return err
	}
	files, err := os.ReadDir(g.dbPath)
	if err != nil {
		return err
	}

	for i := 0; i < 10; i++ {
		levelPrefix := fmt.Sprintf("%v_", i)
		levelFiles := []os.DirEntry{}
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".db") && strings.HasPrefix(f.Name(), levelPrefix) {
				levelFiles = append(levelFiles, f)
			}
		}

		sort.Slice(levelFiles, func(a, b int) bool {
			fA := levelFiles[a].Name()
			fB := levelFiles[b].Name()
			if !strings.HasSuffix(fA, ".db") || !strings.HasSuffix(fB, ".db") {
				return false
			}

			fA = strings.TrimSuffix(fA, ".db")
			fA = strings.TrimPrefix(fA, levelPrefix)
			fB = strings.TrimSuffix(fB, ".db")
			fB = strings.TrimPrefix(fB, levelPrefix)

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

		g.sstableMetadata = append(g.sstableMetadata, []*internal.MetaBlock{})

		for _, f := range levelFiles {
			if !f.IsDir() && strings.HasSuffix(f.Name(), ".db") {
				table := internal.NewSSTable(g.dbPath, f.Name())
				err := table.ReadMeta()
				if err != nil {
					return err
				}
				g.sstableMetadata[i] = append(g.sstableMetadata[i], &internal.MetaBlock{
					FileName: f.Name(),
					MinKey:   table.MinKey,
					MaxKey:   table.MaxKey,
					KeyCount: table.KeyCount,
				})
			}
		}

	}

	return nil
}

func (g *SpaceDBImpl) Set(key []byte, value *DBValue) error {
	g.rwLock.Lock()
	defer g.rwLock.Unlock()

	err := g.walManager.Add(&wal.Log{
		Key:   key,
		Value: value.Serialize(),
	})
	if err != nil {
		log.Println(err)
		return err
	}

	g.curMemTable.Set(key, value.Serialize())

	if g.curMemTable.RawSize() > MaxMemTableSize {
		g.switchMemTable()
	}

	return nil
}

func (g *SpaceDBImpl) Get(key []byte) *DBValue {
	g.rwLock.RLock()
	defer g.rwLock.RUnlock()
	val := g.curMemTable.Get(key)
	if val != nil {
		return Deserialize(val)
	}

	// can't find in memtable, find in sstables
	for _, meta := range g.sstableMetadata {
		for i := len(meta) - 1; i >= 0; i-- {
			m := meta[i]

			// log.Printf("searching in %v min: %v max: %v\n", m.FileName, string(*m.MinKey), string(*m.MaxKey))
			if bytes.Compare(key, *m.MinKey) >= 0 && bytes.Compare(key, *m.MaxKey) <= 0 {
				table := internal.NewSSTable(g.dbPath, m.FileName)
				defer table.CloseFile()
				pos, err := table.FindKeyInIndex(key)
				if err == internal.ErrIndexNotFound {
					continue
				}
				val, err := table.ReadValueAt(pos)
				if err != nil {
					log.Println(err)
					return nil
				}
				return Deserialize(val)
			}
		}

	}

	return nil
}

func (g *SpaceDBImpl) Delete(key []byte) error {
	g.rwLock.Lock()
	defer g.rwLock.Unlock()

	delVal := &DBValue{IsDeleted: true}

	err := g.walManager.Add(&wal.Log{
		Key:   key,
		Value: delVal.Serialize(),
	})
	if err != nil {
		log.Println(err)
		return err
	}

	g.curMemTable.Set(key, delVal.Serialize())

	return nil
}

func (g *SpaceDBImpl) KeyCount() int64 {
	count := g.curMemTable.KeyCount()
	for _, meta := range g.sstableMetadata {
		for _, m := range meta {
			count += m.KeyCount
		}
	}
	return count
}

func (g *SpaceDBImpl) Close() {
	panic("implement me")
}

func (g *SpaceDBImpl) switchMemTable() {
	fileName := fmt.Sprintf("0_%v.db", g.curFileNum)
	table := internal.NewSSTable(g.dbPath, fileName)
	err := table.Save(g.curMemTable)
	if err != nil {
		log.Println(err)
		return
	}

	g.sstableMetadata[0] = append(g.sstableMetadata[0], &internal.MetaBlock{
		FileName: fileName,
		MinKey:   table.MinKey,
		MaxKey:   table.MaxKey,
		KeyCount: table.KeyCount,
	})
	oldWalName := g.walManager.SwitchFile()
	g.clearWAL(oldWalName)
	nMemTable := internal.NewMemTable()
	g.curMemTable = nMemTable
	g.curFileNum++
}

func (g *SpaceDBImpl) clearWAL(path string) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return
	}
	err = os.Remove(path)
	if err != nil {
		log.Println(err)
	}
}
