package datastore

import (
	"fmt"

	"github.com/eolso/threadsafe"
)

// Collection effectively represents a folder. A Collection may contain uniquely named Document(s) or more Collection(s).
type Collection struct {
	name        string
	documents   *threadsafe.Map[string, *Document]
	collections *threadsafe.Map[string, *Collection]
}

type CollectionReader interface {
	Get(key string) *Document
	GetAll() []*Document
}

type CollectionWriter interface {
	Set(key string, document *Document) error
	Delete(key string)
}

type CollectionReadWriter interface {
	CollectionReader
	CollectionWriter
}

func newCollection(name string) *Collection {
	return &Collection{
		name:        name,
		documents:   threadsafe.NewMap[string, *Document](),
		collections: threadsafe.NewMap[string, *Collection](),
	}
}

func (c *Collection) Get(key string) (*Document, bool) {
	return c.documents.Get(key)
}

func (c *Collection) GetAll() []*Document {
	return c.documents.Values()
}

func (c *Collection) Set(key string, document *Document) error {
	if document == nil {
		return fmt.Errorf("cannot insert nil document into collection")
	}

	c.documents.Set(key, document)

	return nil
}

func (c *Collection) Delete(key string) {
	c.documents.Delete(key)
}

func (c *Collection) DeleteCollection(key string) {
	c.collections.Delete(key)
}

// Document is a helper function that returns an existing document if it exists, and creates it if it doesn't.
func (c *Collection) Document(name string) *Document {
	document, ok := c.documents.Get(name)
	if !ok {
		document = NewDocument(name)
		c.documents.Set(name, document)
	}

	return document
}

func (c *Collection) Collection(name string) *Collection {
	collection, ok := c.collections.Get(name)
	if !ok {
		collection = newCollection(name)
		c.collections.Set(name, collection)
	}

	return collection
}
