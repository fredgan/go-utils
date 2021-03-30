package util

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func ContainsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func Contain(array interface{}, val interface{}) bool {
	i := Find(array, val)
	if i == -1 {
		return false
	}
	return true
}

func Find(array interface{}, val interface{}) int {
	v := reflect.ValueOf(array)
	t := reflect.TypeOf(array)
	if t.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}
	if t.Kind() != reflect.Slice {
		panic("Reverse a non-slice type")
	}

	vt := reflect.TypeOf(val)
	if t.Elem() != vt {
		panic("Elem and Val type not match")
	}

	for i := 0; i < v.Len(); i++ {
		ei := v.Index(i)
		if reflect.DeepEqual(ei.Interface(), val) {
			return i
		}
	}

	return -1
}

func Join(array interface{}, sep string) string {
	v := reflect.ValueOf(array)
	t := reflect.TypeOf(array)
	if t.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}
	if t.Kind() != reflect.Slice {
		panic("Join a non-slice type")
	}
	n := v.Len()
	a := make([]string, n)
	for i := 0; i < n; i++ {
		e := v.Index(i)
		a[i] = fmt.Sprintf("%v", e.Interface())
	}
	return strings.Join(a, sep)
}

func JoinInts(a []int64, sep string) string {
	strs := make([]string, len(a))
	for i, each := range a {
		strs[i] = strconv.FormatInt(each, 10)
	}
	return strings.Join(strs, sep)
}

func Reverse(array interface{}) {
	v := reflect.ValueOf(array)
	t := reflect.TypeOf(array)
	if t.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}
	if t.Kind() != reflect.Slice {
		panic("Reverse a non-slice type")
	}

	var tmp reflect.Value = reflect.New(t.Elem()).Elem()

	for i, j := 0, v.Len()-1; i < j; i, j = i+1, j-1 {
		ei := v.Index(i)
		ej := v.Index(j)
		tmp.Set(ei)
		ei.Set(ej)
		ej.Set(tmp)
	}
}

// Remove all elems equals to Val in Array.
// Return the number of elems removed.
func Remove(ptrArray interface{}, val interface{}) int {
	v := reflect.ValueOf(ptrArray)
	t := reflect.TypeOf(ptrArray)
	if t.Kind() != reflect.Ptr {
		panic("Must be a pointer of slice")
	}
	t = t.Elem()
	v = v.Elem()

	if t.Kind() != reflect.Slice {
		panic("Remove a non-slice type")
	}
	if !v.CanAddr() {
		panic("Array can not address")
	}

	vt := reflect.TypeOf(val)
	if t.Elem() != vt {
		panic("Elem and Val type not match")
	}

	var removed int
	for i, j := 0, 0; i < v.Len(); i++ {
		ei := v.Index(i)
		if reflect.DeepEqual(ei.Interface(), val) {
			removed += 1
		} else {
			v.Index(j).Set(ei)
			j += 1
		}
	}

	v.SetLen(v.Len() - removed)

	return removed
}
