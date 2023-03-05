package datastore

import "errors"

// GetWithDefault attempts to get a key from a document, if it doesn't exist the def() will be returned
func GetWithDefault[V any](doc DocumentReader, key string, def func() V) V {
	var v V

	if err := doc.Get(key).Decode(&v); err != nil && errors.Is(err, ErrEmptyItem) {
		return def()
	}

	return v
}

// GetSetWithDefault acts the same as GetWithDefault except it will also write the value of def() to the document if
// the key was not found.
func GetSetWithDefault[V any](doc DocumentReadWriter, key string, def func() V) (V, error) {
	var v V
	var err error

	if err = doc.Get(key).Decode(&v); err != nil && errors.Is(err, ErrEmptyItem) {
		v = def()
		err = doc.Set(key, v)
	}

	return v, err
}
