package main

import (
	"fmt"
)

func main() {
	// old := uint64(1 << 63)
	idx := 3
	v := uint64(0b1110111)
	// l := 2
	// mask := ^(^uint64(0) >> (64 - l - idx) & (^uint64(0) << idx))
	// fmt.Printf("%064b\n", mask)
	fmt.Printf("%064b\n", v)
	fmt.Printf("%064b\n", v>>idx)
	// fmt.Printf("%064b\n", (v&^mask)|(old&mask))
	// buf := bytes.NewBuffer(nil)
	// buf2 := bytes.NewBuffer(nil)
	// for i := 0; i < 1_000_000; i++ {
	// 	raw := make([]byte, 8)
	// 	binary.LittleEndian.PutUint64(raw, uint64(i))
	// 	buf.Write(raw)
	// 	encoded := make([]byte, binary.MaxVarintLen64)
	// 	n := binary.PutUvarint(encoded, uint64(i))
	// 	buf2.Write(encoded[:n])
	// }

	// PrintBytes("raw", buf.Len())
	// PrintBytes("encoded", buf2.Len())

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
