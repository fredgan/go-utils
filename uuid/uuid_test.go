package uuid

import (
	"testing"
)

func TestBinString(t *testing.T) {
	u := NewUUID().BinString()
	if len(u) != 16 {
		t.Fatal("invalid len ", len(u))
	}
}
