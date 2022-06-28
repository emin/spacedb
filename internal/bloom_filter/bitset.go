package bloomfilter

type BitSet struct {
	bits  []uint64
	index int64
}

func NewBitSet() *BitSet {
	return &BitSet{bits: make([]uint64, 0)}
}

func (b *BitSet) Get(index int) bool {
	if index < 0 {
		return false
	}
	if int64(index) >= b.Size() {
		return false
	}
	arIdx := int(index / 64)
	bitIdx := int(index % 64)
	return (b.bits[arIdx] & (1 << bitIdx)) != 0
}

func (b *BitSet) GetRange(index int, len int) uint64 {
	if index < 0 {
		return 0
	}
	if int64(index) >= b.Size() {
		return 0
	}
	arIdx := int(index / 64)
	bitIdx := int(index % 64)
	rem := len
	val := uint64(0)
	offset := 0
	for rem > 0 {
		l := 64 - bitIdx
		if l > rem {
			l = rem
		}
		mask := ^(^uint64(0) >> (64 - (l + bitIdx)) & (^uint64(0) << bitIdx))
		v := b.bits[arIdx] & mask
		nVal := v >> bitIdx
		nVal <<= offset
		val |= nVal
		rem -= l
		offset += l
		arIdx++
		bitIdx = 0
	}
	return uint64(val)
}

func (b *BitSet) Add(value bool) {
	if b.index >= b.Size() {
		b.bits = append(b.bits, 0)
	}
	b.Set(b.index, value)
	b.index++
}

func (b *BitSet) AddRange(val uint64, len int) {
	if b.index+int64(len) > b.Size() {
		b.bits = append(b.bits, 0)
	}
	b.SetRange(b.index, len, val)
	b.index += int64(len)
}

func (b *BitSet) Set(index int64, value bool) {
	if index < 0 {
		return
	}
	if index >= b.Size() {
		return
	}
	arIdx := int(index / 64)
	bitIdx := int(index % 64)
	if value {
		b.bits[arIdx] = setBit(b.bits[arIdx], bitIdx)
	} else {
		b.bits[arIdx] = resetBit(b.bits[arIdx], bitIdx)
	}
}

func (b *BitSet) SetRange(index int64, len int, value uint64) {
	if index < 0 {
		return
	}
	if index >= b.Size() {
		return
	}

	arIdx := int(index / 64)
	bitIdx := int(index % 64)
	val := value
	rem := len
	for rem > 0 {
		l := 64 - bitIdx
		if l > rem {
			l = rem
		}
		mask := ^(^uint64(0) >> (64 - (l + bitIdx)) & (^uint64(0) << bitIdx))
		v := val << bitIdx
		b.bits[arIdx] = (b.bits[arIdx] & mask) | (v & ^mask)
		rem -= l
		arIdx++
		bitIdx = 0
		val >>= l
	}

}

func (b *BitSet) Size() int64 {
	return int64(len(b.bits) * 64)
}

func setBit(num uint64, idx int) uint64 {
	return num | (1 << idx)
}

func resetBit(num uint64, idx int) uint64 {
	return num & ^(1 << idx)
}
