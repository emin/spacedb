package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/edsrzf/mmap-go"
)

func mainTest() {
	// writeNormal()
	// writeBufIO()
	writeMmap()
}

func writeMmap() {
	a, err := os.OpenFile("test.txt", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer a.Close()

	m, err := mmap.Map(a, mmap.RDWR, 0)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 10_000_000; i++ {
		m[i] = byte('b')
	}

	if err := m.Unmap(); err != nil {
		log.Fatal(err)
	}

}

func writeBufIO() {
	a, err := os.Create("test.txt")
	if err != nil {
		log.Fatal(err)
	}

	w := bufio.NewWriter(a)
	for i := 0; i < 10_000_000; i++ {
		k := fmt.Sprintf("key-value->%d", i)
		w.Write([]byte(k))
	}
	w.Flush()
	a.Close()
}

func writeNormal() {
	a, err := os.Create("test.txt")
	if err != nil {
		log.Fatal(err)
	}

	w := io.Writer(a)
	for i := 0; i < 10_000_000; i++ {
		k := fmt.Sprintf("key-value->%d", i)
		w.Write([]byte(k))
	}
	a.Close()
}
