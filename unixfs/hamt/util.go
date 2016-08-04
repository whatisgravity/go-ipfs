package hamt

import (
	"math/big"
)

type hashBits struct {
	b        []byte
	consumed int
}

func (hb *hashBits) Next(i int) int {
	if i%8 != 0 {
		panic("cant currently do uneven bitsizes")
	}

	frag := hb.b[hb.consumed/8 : (hb.consumed+i)/8]
	hb.consumed += i

	return toInt(frag)
}

func toInt(b []byte) int {
	var s int
	for i, v := range b {
		s += int(v << uint(i*8))
	}
	return s
}

const (
	m1  = 0x5555555555555555 //binary: 0101...
	m2  = 0x3333333333333333 //binary: 00110011..
	m4  = 0x0f0f0f0f0f0f0f0f //binary:  4 zeros,  4 ones ...
	h01 = 0x0101010101010101 //the sum of 256 to the power of 0,1,2,3...
)

// from https://en.wikipedia.org/wiki/Hamming_weight
func popCountUint64(x uint64) int {
	x -= (x >> 1) & m1             //put count of each 2 bits into those 2 bits
	x = (x & m2) + ((x >> 2) & m2) //put count of each 4 bits into those 4 bits
	x = (x + (x >> 4)) & m4        //put count of each 8 bits into those 8 bits
	return int((x * h01) >> 56)
}

func popCount(i *big.Int) int {
	var n int
	for _, v := range i.Bits() {
		n += popCountUint64(uint64(v))
	}
	return n
}
