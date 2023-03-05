package datastore

import (
	"errors"
	"reflect"
)

var ErrKeyNotFound = errors.New("datastore: key not found")
var ErrEmptyItem = errors.New("datastore: Decode(empty item")
var ErrInvalidPath = errors.New("datastore: path must be a directory")

type ErrInvalidDecode struct {
	Type reflect.Type
}

func (e ErrInvalidDecode) Error() string {
	if e.Type == nil {
		return "datastore: Decode(nil)"
	}

	if e.Type.Kind() != reflect.Pointer {
		return "datastore: Decode(non-pointer " + e.Type.String() + ")"
	}

	return "datastore: Decode(nil " + e.Type.String() + ")"
}
