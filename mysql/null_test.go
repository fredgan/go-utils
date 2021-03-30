package mysql

import (
	"encoding/json"
	"testing"
)

type testNullStruct struct {
	Int    NullInt64   `json:"int"`
	String NullString  `json:"str"`
	Float  NullFloat64 `json:"float"`
	Bool   NullBool    `json:"bool"`
}

func TestNullStringMarshal(t *testing.T) {
	s := testNullStruct{}
	r, err := json.Marshal(&s)
	if err != nil {
		t.Fatal(err.Error())
	}

	if string(r) != `{"int":0,"str":"","float":0,"bool":false}` {
		t.Fatal("invalid value ", string(r))
	}
}
