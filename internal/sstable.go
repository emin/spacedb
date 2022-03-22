package internal

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"path"

	"github.com/emin/go-kv-db/helpers"
)

// echo gorocks | sha256sum
// b9ebfbc20ef278a235a18f4fc160e1038a8e8a2ab90f4978666bab2203d4bfdf
const MagicNumber uint32 = 0x3d4bfdf

type SSTable struct {
	dbPath      string
	name        string
	file        *os.File
	footerBlock *FooterBlock
	MinKey      *[]byte
	MaxKey      *[]byte
	KeyCount    int64
}

type FooterBlock struct {
	DataOffset  int64
	DataLength  int64
	IndexOffset int64
	IndexLength int64
	MetaOffset  int64
	MetaLength  int64
}

type IndexBlock struct {
	Pos int64
	Key []byte
}

type MetaBlock struct {
	MinKey   *[]byte
	MaxKey   *[]byte
	FileName string
	KeyCount int64
}

func NewSSTable(dbPath string, name string) *SSTable {
	return &SSTable{
		dbPath: dbPath,
		name:   name,
	}
}

//
//	SSTable format on Disk
//
//	  ---------------
//   |   Data Block  |
//   |---------------|
//   |  Index Block  |
//   |---------------|
//	 |   Meta Block  |
//   |---------------|
//   |    Footer     |
//    ---------------
//
//     Data Block
//   ------------------------------------------
//  | Key Len (4-bytes) | Key |  Value Len (4-bytes) | Value | .... |
//   ------------------------------------------
//
//     Index Block
//   ----------------------------------
//  | Key Len (4-bytes) | Key | Position (8-bytes) | .... |
//   ----------------------------------
//
//     Meta Block
//   ------------------------------------------------------
//  | Min Key Len (4-bytes) | Min Key | Max Key Len (4-bytes) | Max Key | Key Count (8-bytes) | .... |
//   ------------------------------------------------------
//
//     Footer
//   ----------------------------------------------------------------------------------------
//  | Data Len (8-bytes) | Index Len (8-bytes) | Meta Len (4-bytes) | Magic Number (4-bytes) |
//   ----------------------------------------------------------------------------------------
//
// TODO: add key count to meta block
// TODO: add creation order index to meta block
// TODO: add compression support
// TODO: variable length ints

func (t *SSTable) Save(table MemTable) error {
	fPath := path.Join(t.dbPath, t.name)
	file, err := os.Create(fPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	indexes := make([]*IndexBlock, 0)
	w := bufio.NewWriter(file)
	it := table.Iterator()
	// write data block
	var pos int64 = 0
	for it.Next() {
		key := it.Key()
		val := it.Value()
		err = helpers.WriteUint32(w, uint32(len(key)))
		if err != nil {
			return err
		}
		_, err = w.Write(key)
		if err != nil {
			return err
		}

		err = helpers.WriteUint32(w, uint32(len(val)))
		if err != nil {
			return err
		}
		_, err = w.Write(val)
		if err != nil {
			return err
		}
		indexes = append(indexes, &IndexBlock{
			Key: key,
			Pos: pos,
		})
		pos += int64(4 + len(key) + 4 + len(val))
	}
	dataLen := pos
	// write index block
	pos = 0
	for _, idx := range indexes {
		err = helpers.WriteUint32(w, uint32(len(idx.Key)))
		if err != nil {
			return err
		}
		n, err := w.Write(idx.Key)
		if err != nil {
			return err
		}
		if n != len(idx.Key) {
			return errors.New("write index key error")
		}

		err = helpers.WriteUint64(w, uint64(idx.Pos))
		if err != nil {
			return err
		}

		pos += int64(4 + len(idx.Key) + 8)
	}
	indexLen := pos

	// write meta block
	minKey := indexes[0].Key
	maxKey := indexes[len(indexes)-1].Key
	t.MinKey = &minKey
	t.MaxKey = &maxKey
	t.KeyCount = int64(len(indexes))

	err = helpers.WriteUint32(w, uint32(len(minKey)))
	if err != nil {
		return err
	}
	_, err = w.Write(minKey)
	if err != nil {
		return err
	}

	err = helpers.WriteUint32(w, uint32(len(maxKey)))
	if err != nil {
		return err
	}
	_, err = w.Write(maxKey)
	if err != nil {
		return err
	}

	err = helpers.WriteUint64(w, uint64(t.KeyCount))
	if err != nil {
		return err
	}

	// write footer
	metaLen := 4 + len(minKey) + 4 + len(maxKey) + 8

	err = helpers.WriteUint64(w, uint64(dataLen))
	if err != nil {
		return err
	}

	err = helpers.WriteUint64(w, uint64(indexLen))
	if err != nil {
		return err
	}

	err = helpers.WriteUint32(w, uint32(metaLen))
	if err != nil {
		return err
	}

	err = helpers.WriteUint32(w, MagicNumber)
	if err != nil {
		return err
	}

	err = w.Flush()

	return err
}

func (t *SSTable) openForRead() error {
	fPath := path.Join(t.dbPath, t.name)
	f, err := os.OpenFile(fPath, os.O_RDONLY, os.ModeType)
	if err != nil {
		return err
	}
	t.file = f
	return nil
}

func (t *SSTable) CloseFile() {
	if t.file != nil {
		_ = t.file.Close()
	}
}

func (t *SSTable) ReadValueAt(pos uint64) ([]byte, error) {
	if t.file == nil {
		err := t.openForRead()
		if err != nil {
			return nil, err
		}
	}
	_, err := t.file.Seek(int64(pos), 0)
	if err != nil {
		return nil, err
	}
	keyLen, err := helpers.ReadUint32(t.file)
	if err != nil {
		return nil, err
	}
	key := make([]byte, keyLen)
	_, err = io.ReadFull(t.file, key)
	if err != nil {
		return nil, err
	}
	valLen, err := helpers.ReadUint32(t.file)
	if err != nil {
		return nil, err
	}
	val := make([]byte, valLen)
	_, err = io.ReadFull(t.file, val)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (t *SSTable) FindKeyInIndex(key []byte) (uint64, error) {
	if t.footerBlock == nil {
		err := t.ReadFooter()
		if err != nil {
			return 0, err
		}
	}
	_, err := t.file.Seek(int64(t.footerBlock.IndexOffset), 0)
	if err != nil {
		return 0, err
	}

	rdr := bufio.NewReader(t.file)

	c := int64(0)
	idxLen := int64(t.footerBlock.IndexLength)
	i := 0

	defer func() {
		log.Printf("Looked up %d keys in %v\n", i, t.file.Name())
	}()

	for c < idxLen {

		keyLen, err := helpers.ReadUint32(rdr)
		if err != nil {
			return 0, err
		}

		curKey := make([]byte, keyLen)

		n, err := io.ReadFull(rdr, curKey)

		if err != nil {
			return 0, err
		}
		if n != int(keyLen) {
			return 0, ErrIndexReadError
		}
		if bytes.Equal(key, curKey) {
			pos, err := helpers.ReadUint64(rdr)
			if err != nil {
				return 0, err
			}
			return pos, nil
		} else {
			rdr.Discard(8)
		}
		c += int64(keyLen) + 12 // key len + key + pos
		i++
	}
	return 0, ErrIndexNotFound
}

func (t *SSTable) ReadMeta() error {
	if t.footerBlock == nil {
		err := t.ReadFooter()
		if err != nil {
			return err
		}
	}
	_, err := t.file.Seek(int64(t.footerBlock.MetaOffset), 0)
	if err != nil {
		return err
	}
	rdr := bufio.NewReader(t.file)
	minKey, err := helpers.ReadSlice(rdr)
	if err != nil {
		return err
	}
	t.MinKey = minKey
	maxKey, err := helpers.ReadSlice(rdr)
	if err != nil {
		return err
	}
	t.MaxKey = maxKey

	keyCount, err := helpers.ReadUint64(rdr)
	if err != nil {
		return err
	}
	t.KeyCount = int64(keyCount)

	return nil
}

func (t *SSTable) ReadFooter() error {
	if t.file == nil {
		err := t.openForRead()
		if err != nil {
			return err
		}
	}
	_, err := t.file.Seek(-24, 2)
	if err != nil {
		return err
	}
	rdr := bufio.NewReader(t.file)
	dataLen, err := helpers.ReadUint64(rdr)
	if err != nil {
		return err
	}
	indexLen, err := helpers.ReadUint64(rdr)
	if err != nil {
		return err
	}
	metaLen, err := helpers.ReadUint32(rdr)
	if err != nil {
		return err
	}
	magicNum, err := helpers.ReadUint32(rdr)
	if err != nil {
		return err
	}

	if magicNum != MagicNumber {
		return errors.New("magic number doesn't match")
	}

	t.footerBlock = &FooterBlock{
		DataOffset:  0,
		DataLength:  int64(dataLen),
		IndexOffset: int64(dataLen),
		IndexLength: int64(indexLen),
		MetaOffset:  int64(dataLen + indexLen),
		MetaLength:  int64(metaLen),
	}

	return nil
}
