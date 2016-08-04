package hamt

import (
	"math/big"
	"testing"
)

func TestPopCount(t *testing.T) {
	x := big.NewInt(0)

	for i := 0; i < 50; i++ {
		x.SetBit(x, i, 1)
	}

	/*
		if popCount(x) != 50 {
			t.Fatal("expected popcount to be 50")
		}
	*/
}
