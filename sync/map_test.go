package sync

import (
	"bytes"
	"io"
	"testing"
)

func TestSync2Map(t *testing.T) {
	m := NewMap()

	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	if v, ok := m.Get("a"); !ok || v != 1 {
		t.Fatal("get failed", v, ok)
	}
	if v, ok := m.Get("b"); !ok || v != 2 {
		t.Fatal("get failed", v, ok)
	}
	if v, ok := m.Get("c"); !ok || v != 3 {
		t.Fatal("get failed", v, ok)
	}

	m.Del("b")

	if m.Len() != 2 {
		t.Fatal("len != 2", m.Len())
	}
	if v, ok := m.Get("b"); ok {
		t.Fatal("del failed", v, ok)
	}

	keys, vals := m.KeyVals()
	if len(keys) != 2 || len(keys) != len(vals) {
		t.Fatal("key vals failed", keys, vals)
	}

	keys, vals = nil, nil
	m.For(func(k interface{}, v interface{}) bool {
		keys = append(keys, k)
		vals = append(vals, v)
		return true
	})

	/////////////

	m.Set("buf", &bytes.Buffer{})

	var w io.ReadWriter

	m.GetTo("buf", &w)
}

func TestSync2Int64Map(t *testing.T) {
	m := NewInt64Map()

	m.Set("aa", 1000)

	n := int64(1000)
	for i := int64(0); i < 100; i++ {
		m.Add("aa", i)
		n += i
	}

	if v, ok := m.Get("aa"); !ok || v != n {
		t.Fatal("get failed", v, ok)
	}
	if m.Len() != 1 {
		t.Fatal("len != 1", m.Len())
	}

}
