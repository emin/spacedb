package main

import (
	"bytes"
	"fmt"
)

func maint() {
	buf := bytes.NewBuffer(nil)
	for i := 0; i < 1_000_000; i++ {
		k := fmt.Sprintf("k%d", i)
		buf.Write([]byte(k))
	}

	fmt.Println("Len ", buf.Len())
}
