package mysql

import (
	"testing"
)

func TestStatus(t *testing.T) {
	s := &Status{}
	s.QueryCount.Add(1)
	r := s.QueryCount.Get()
	if r != 1 {
		t.Fatal("invalid value ", r)
	}
	s.QueryCount.Add(1)
	r = s.QueryCount.Get()
	if r != 2 {
		t.Fatal("invalid value ", r)
	}
}
