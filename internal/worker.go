package internal

import (
	"fmt"
	"log"
	"os"

	"github.com/emin/go-kv-db/internal/wal"
)

type SwitchRequest struct {
	memTable   MemTable
	lastWalNum int
	curFileNum int
}

type Worker struct {
	queue      chan SwitchRequest
	dbPath     string
	walManager *wal.Manager
}

func NewWorker(dbPath string, walManager *wal.Manager) *Worker {
	return &Worker{queue: make(chan SwitchRequest), dbPath: dbPath, walManager: walManager}
}

func (w *Worker) Start() {
	go func() {
		for {
			req := <-w.queue
			fileName := fmt.Sprintf("%v.db", req.curFileNum)
			table := NewSSTable(w.dbPath, fileName)
			err := table.Save(req.memTable)
			if err != nil {
				log.Println(err)
				continue
			}
			w.ClearWAL(req.lastWalNum)
		}
	}()
}

func (w *Worker) ClearWAL(num int) {
	path := fmt.Sprintf("%v/wal/%v.log", w.dbPath, num)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return
	}
	err = os.Remove(path)
	if err != nil {
		log.Println(err)
	}
}

func (w *Worker) Add(m MemTable, curWalNum, curFileNum int) {
	go func() {
		w.queue <- SwitchRequest{
			memTable:   m,
			lastWalNum: curWalNum,
			curFileNum: curFileNum,
		}
	}()
}
