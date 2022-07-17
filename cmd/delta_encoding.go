package main

import (
	"bytes"
	"fmt"
)

func main() {

	buf := bytes.NewBuffer(nil)

	for i := 0; i < 10_000_000; i++ {
		buf.Write([]byte(fmt.Sprintf("k%v", i)))
		buf.Write([]byte(fmt.Sprintf("value = %v", i)))
	}

	PrintBytes("raw", buf.Len())

	// for i := 0; i < 1_000_000; i++ {
	// 	n, err := binary.ReadUvarint(buf2)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	if n != uint64(i) {
	// 		log.Fatalf("%v != %v\n", n, i)
	// 	}
	// }

}

func PrintBytes(desc string, l int) {
	fmt.Printf("%v -> Len %v KiB \n", desc, l/1024.0)
}
