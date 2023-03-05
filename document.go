package datastore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"

	"github.com/eolso/threadsafe"
)

type Item struct {
	Key   string
	Value []byte
}

type Document struct {
	Name string
	data *threadsafe.Map[string, []byte]
}

type DocumentReader interface {
	Get(key string) Item
	GetAll() []Item
}

type DocumentWriter interface {
	Set(key string, value interface{}) error
	Delete(key string)
}

type DocumentReadWriter interface {
	DocumentReader
	DocumentWriter
}

func (i Item) Decode(v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return &ErrInvalidDecode{reflect.TypeOf(v)}
	}

	if len(i.Value) == 0 {
		return ErrEmptyItem
	}

	return gob.NewDecoder(bytes.NewBuffer(i.Value)).Decode(v)
}

func NewDocument(name string) *Document {
	return &Document{
		Name: name,
		data: threadsafe.NewMap[string, []byte](),
	}
}

func (d *Document) Get(key string) Item {
	b, ok := d.data.Get(key)
	if !ok {
		return Item{}
	}

	return Item{Key: key, Value: b}
}

func (d *Document) GetAll() []Item {
	items := make([]Item, d.data.Len())
	for k, v := range d.data.Data {
		items = append(items, Item{Key: k, Value: v})
	}

	return items
}

func (d *Document) Set(key string, value interface{}) error {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(value); err != nil {
		return fmt.Errorf("could not store value in document: %w", err)
	}

	d.data.Set(key, b.Bytes())

	return nil
}

func (d *Document) RawSet(key string, b []byte) {
	d.data.Set(key, b)
}

func (d *Document) Delete(key string) {
	d.data.Delete(key)
}
