package bloomfilter

import (
	"fmt"
	"testing"
)

func Test_setBit(t *testing.T) {
	for i := 0; i < 64; i++ {
		num := uint64(0)
		num = setBit(num, i)
		want := uint64(1 << uint64(i))
		if num != want {
			t.Errorf("setBit(%d, %d) = %d, want %d", num, i, num, want)
		}
	}

}

func Test_resetBit(t *testing.T) {

	type arg struct {
		num  uint64
		want uint64
		idx  int
	}

	args := []arg{
		{
			num:  uint64(0b11111111),
			want: uint64(0b11011111),
			idx:  5,
		},
		{
			num:  uint64(0b00010000),
			want: uint64(0b00000000),
			idx:  4,
		},
	}
	for _, a := range args {
		num := resetBit(a.num, a.idx)
		if num != a.want {
			t.Errorf("resetBit(%d, %d) = %d, want %d", num, a.want, num, a.want)
		}
	}
}

func Test_Set(t *testing.T) {
	set := BitSet{bits: make([]uint64, 2)}
	set.Set(64, true)

	if set.bits[1] != 1 {
		t.Errorf("set.bits[1] = %v, want 1", set.bits[1])
	}

	set.Set(65, true)
	if set.bits[1] != 3 {
		t.Errorf("set.bits[1] = %v, want 3", set.bits[1])
	}

	set.Set(1, true)
	if set.bits[0] != 2 {
		t.Errorf("set.bits[0] = %v, want 2", set.bits[1])
	}
}

func Test_SetRange(t *testing.T) {
	set := BitSet{bits: make([]uint64, 3)}
	set.SetRange(0, 8, 0b11010001)
	v1 := fmt.Sprintf("%064b", set.bits[0])
	w1 := "0000000000000000000000000000000000000000000000000000000011010001"
	if v1 != w1 {
		t.Errorf("set.bits[0] = %v, want %v", v1, w1)
	}
	set.SetRange(60, 8, 0b11010001)
	v1 = fmt.Sprintf("%064b", set.bits[0])
	w1 = "0001000000000000000000000000000000000000000000000000000011010001"
	w2 := "0000000000000000000000000000000000000000000000000000000000001101"
	v2 := fmt.Sprintf("%064b", set.bits[1])
	if v1 != w1 {
		t.Errorf("set.bits[0] = %v, want %v", v1, w1)
	}
	if v2 != w2 {
		t.Errorf("set.bits[0] = %v, want %v", v2, w2)
	}
}

func Test_Add(t *testing.T) {
	set := NewBitSet()
	for i := 0; i < 128; i++ {
		set.Add(true)
	}

	if set.Size() != 128 {
		t.Errorf("Size() = %d, want %d", set.Size(), 128)
	}

	set.Add(false)
	if set.index != 129 {
		t.Errorf("index = %d, want %d", set.index, 129)
	}
}

func Test_AddRange(t *testing.T) {
	set := NewBitSet()
	for i := 0; i < 128; i++ {
		set.AddRange(0b11010001, 8)
	}
	if set.Size() != 128*8 {
		t.Errorf("Size() = %d, want %d", set.Size(), 128*8)
	}
}

func Test_Size(t *testing.T) {
	set := BitSet{bits: make([]uint64, 2)}
	if set.Size() != 128 {
		t.Errorf("Size() = %d, want %d", set.Size(), 128)
	}
}

func Test_GetRange(t *testing.T) {
	set := NewBitSet()
	v := "00000000000000000000000000000000000000000000000000000000000011010001000000000000000000000000000000000000000000000000000011010001"

	for i := len(v) - 1; i >= 0; i-- {
		if v[i] == '1' {
			set.Add(true)
		} else {
			set.Add(false)
		}
	}

	want := uint64(0b11010001)

	if got := set.GetRange(60, 8); got != want {
		t.Errorf("GetRange(60, 8) = %064b, want %064b", got, want)
	}

	want = uint64(0b00001101)

	if got := set.GetRange(4, 4); got != want {
		t.Errorf("GetRange(4, 4) = %064b, want %064b", got, want)
	}

}

func Test_Get(t *testing.T) {
	set := NewBitSet()
	for i := 0; i < 128; i++ {
		if i == 125 {
			set.Add(true)
		} else {
			set.Add(false)
		}
	}
	for i := 0; i < 128; i++ {
		if i == 125 {
			if !set.Get(i) {
				t.Errorf("Get(%d) = %v, want %v", i, set.Get(i), true)
			}
		} else {
			if set.Get(i) {
				t.Errorf("Get(%d) = %v, want %v", i, set.Get(i), false)
			}
		}
	}
}
