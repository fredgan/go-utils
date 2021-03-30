package sync

import (
	"fmt"
	"reflect"
	"sync"
)

type Map struct {
	l sync.RWMutex
	m map[interface{}]interface{}
}

func NewMap() *Map {
	m := new(Map)
	m.m = make(map[interface{}]interface{})
	return m
}

func (m *Map) Get(key interface{}) (interface{}, bool) {
	m.l.RLock()
	v, ok := m.m[key]
	m.l.RUnlock()
	return v, ok
}

func (m *Map) GetTo(key interface{}, ptr interface{}) bool {
	v, ok := m.Get(key)
	if !ok {
		return false
	}

	assign(ptr, v) // *ptr = v

	return true
}

func (m *Map) Set(key interface{}, val interface{}) {
	m.l.Lock()
	m.m[key] = val
	m.l.Unlock()
}

func (m *Map) Del(key interface{}) {
	m.l.Lock()
	delete(m.m, key)
	m.l.Unlock()
}

func (m *Map) Len() int {
	m.l.RLock()
	n := len(m.m)
	m.l.RUnlock()
	return n
}

func (m *Map) For(f func(k interface{}, v interface{}) bool) {
	m.l.RLock()
	var ok bool
	for k, v := range m.m {
		ok = f(k, v)
		if !ok {
			break
		}
	}
	m.l.RUnlock()
}

func (m *Map) Keys() []interface{} {
	m.l.RLock()
	keys := make([]interface{}, 0, len(m.m))
	for k, _ := range m.m {
		keys = append(keys, k)
	}
	m.l.RUnlock()
	return keys
}

func (m *Map) Vals() []interface{} {
	m.l.RLock()
	vals := make([]interface{}, 0, len(m.m))
	for _, v := range m.m {
		vals = append(vals, v)
	}
	m.l.RUnlock()
	return vals
}

func (m *Map) KeyVals() ([]interface{}, []interface{}) {
	m.l.RLock()
	keys := make([]interface{}, 0, len(m.m))
	vals := make([]interface{}, 0, len(m.m))
	for k, v := range m.m {
		keys = append(keys, k)
		vals = append(vals, v)
	}
	m.l.RUnlock()
	return keys, vals
}

///////////////////////////////////////////////////////////////////////////////

type Int64Map struct {
	l sync.RWMutex
	m map[interface{}]int64
}

func NewInt64Map() *Int64Map {
	m := new(Int64Map)
	m.m = make(map[interface{}]int64)
	return m
}

func (m *Int64Map) Get(key interface{}) (int64, bool) {
	m.l.RLock()
	v, ok := m.m[key]
	m.l.RUnlock()
	return v, ok
}

func (m *Int64Map) Set(key interface{}, val int64) {
	m.l.Lock()
	m.m[key] = val
	m.l.Unlock()
}

func (m *Int64Map) Add(key interface{}, delta int64) int64 {
	m.l.Lock()
	v, ok := m.m[key]
	if ok {
		v += delta
	} else {
		v = delta
	}
	m.m[key] = v
	m.l.Unlock()
	return v
}

func (m *Int64Map) Del(key interface{}) {
	m.l.Lock()
	delete(m.m, key)
	m.l.Unlock()
}

func (m *Int64Map) Len() int {
	m.l.RLock()
	n := len(m.m)
	m.l.RUnlock()
	return n
}

func (m *Int64Map) For(f func(k interface{}, v int64) bool) {
	m.l.RLock()
	var ok bool
	for k, v := range m.m {
		ok = f(k, v)
		if !ok {
			break
		}
	}
	m.l.RUnlock()
}

func (m *Int64Map) Keys() []interface{} {
	m.l.RLock()
	keys := make([]interface{}, 0, len(m.m))
	for k, _ := range m.m {
		keys = append(keys, k)
	}
	m.l.RUnlock()
	return keys
}

func (m *Int64Map) Vals() []int64 {
	m.l.RLock()
	vals := make([]int64, 0, len(m.m))
	for _, v := range m.m {
		vals = append(vals, v)
	}
	m.l.RUnlock()
	return vals
}

func (m *Int64Map) KeyVals() ([]interface{}, []int64) {
	m.l.RLock()
	keys := make([]interface{}, 0, len(m.m))
	vals := make([]int64, 0, len(m.m))
	for k, v := range m.m {
		keys = append(keys, k)
		vals = append(vals, v)
	}
	m.l.RUnlock()
	return keys, vals
}

///////////////////////////////////////////////////////////////////////////////

func assign(ptr interface{}, val interface{}) {
	pv := reflect.ValueOf(ptr)
	if pv.Kind() != reflect.Ptr {
		panic("ptr must be a pointer")
	}
	ev := pv.Elem()
	if !ev.CanSet() {
		panic("elem of ptr can not be set")
	}

	vv := reflect.ValueOf(val)

	if !vv.Type().AssignableTo(ev.Type()) {
		panic(fmt.Sprintf("type not match (%v, %v)", vv.Type().String(), ev.Type().String()))
	}

	ev.Set(vv)
}
