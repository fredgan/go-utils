package mysql

import (
	"database/sql"
	"encoding/json"
)

type NullString struct {
	sql.NullString
}

func (n *NullString) MarshalJSON() ([]byte, error) {
	return json.Marshal(&n.NullString.String)
}

type NullInt64 struct {
	sql.NullInt64
}

func (n *NullInt64) MarshalJSON() ([]byte, error) {
	return json.Marshal(&n.NullInt64.Int64)
}

type NullBool struct {
	sql.NullBool
}

func (n *NullBool) MarshalJSON() ([]byte, error) {
	return json.Marshal(&n.NullBool.Bool)
}

type NullFloat64 struct {
	sql.NullFloat64
}

func (n *NullFloat64) MarshalJSON() ([]byte, error) {
	return json.Marshal(&n.NullFloat64.Float64)
}
