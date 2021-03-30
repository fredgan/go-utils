package util

import (
	"encoding/json"
	"reflect"
)

func BeanCopy(src, dst interface{}) error {
	srcRef := reflect.ValueOf(src).Elem()
	dstRef := reflect.ValueOf(dst).Elem()

	for i := 0; i < srcRef.NumField(); i++ {
		field := srcRef.Field(i)
		fieldType := field.Type()
		fieldName := srcRef.Type().Field(i).Name

		dstField := dstRef.FieldByName(fieldName)
		if !dstField.IsValid() {
			continue
		}
		if dstField.Type() != fieldType {
			continue
		}
		dstField.Set(field)
	}

	return nil
}

func BeanCopyByJson(source, result interface{}) error {
	bytes, err := json.Marshal(source)
	if nil != err {
		return err
	}

	err = json.Unmarshal(bytes, result)
	if nil != err {
		return err
	}

	return nil
}
