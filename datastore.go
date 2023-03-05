package datastore

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"sync"

	"github.com/eolso/threadsafe"
)

// Datastore is the top level mechanism in charge of reading and writing the documents on disk. Datastore(s) contain
// Collection(s) only, and cannot directly hold a Document.
type Datastore struct {
	path        string
	collections *threadsafe.Map[string, *Collection]
	lock        sync.Mutex
}

// NewDatastore creates and returns a *Datastore with path. In most cases, Open should be called instead as this
// constructor will not attempt to read any Datastore from disk.
func NewDatastore(path string) *Datastore {
	return &Datastore{
		path:        filepath.Clean(path),
		collections: threadsafe.NewMap[string, *Collection](),
	}
}

// Collection returns the *Collection named name. If it doesn't exist a new one will be created and returned.
func (d *Datastore) Collection(name string) *Collection {
	collection, ok := d.collections.Get(name)
	if !ok {
		collection = newCollection(name)
		d.collections.Set(name, collection)
	}

	return collection
}

// Open reads path for an existing Datastore and returns it. If one does not exist, it will be created and returned.
// If the program does not have permissions to read/write to the path specified, this will return an error.
func Open(path string) (*Datastore, error) {
	datastore := NewDatastore(path)
	//baseDepth := strings.Count(path, string(os.PathSeparator))

	// If the directory does not exist, create it and return an empty Datastore
	stat, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return datastore, os.MkdirAll(path, 0700)
	} else if err != nil {
		return datastore, err
	}

	// Datastore must be a directory
	if !stat.IsDir() {
		return datastore, ErrInvalidPath
	}

	// Iterate over all the Collections (directories) in the Datastore
	entries, err := os.ReadDir(path)
	if err != nil {
		return datastore, err
	}

	// Define a recursive function for traversing a collection
	var traverseCollection func(*Collection, *Collection, string) error
	traverseCollection = func(collection *Collection, previousCollection *Collection, path string) error {
		// Check if the current path is a Document
		pathEntries, err := os.ReadDir(path)
		if err != nil {
			return err
		}

		isDocument := false
		isCollection := false
		for _, pathEntry := range pathEntries {
			if pathEntry.IsDir() {
				isCollection = true
			} else {
				isDocument = true
			}
		}

		// A directory can't be both a Document and a Collection.
		if isDocument && isCollection {
			return ErrInvalidPath
		}

		// If it is a Document, create the Document in the Collection and read it in.
		if isDocument {
			// This shouldn't trigger since a Datastore can never contain Documents, but let's prevent a panic just
			// in case.
			if previousCollection == nil {
				return ErrInvalidPath
			}

			previousCollection.DeleteCollection(filepath.Base(path))
			document := previousCollection.Document(filepath.Base(path))

			for _, documentEntry := range pathEntries {
				b, err := os.ReadFile(filepath.Join(path, documentEntry.Name()))
				if err != nil {
					return err
				}

				decodedBytes, err := base64.StdEncoding.DecodeString(string(b))
				if err != nil {
					return err
				}

				document.RawSet(documentEntry.Name(), decodedBytes)
			}

			return nil
		} else if isCollection {
			// This isn't a document. Create a Collection in the current Collection, and continue traversing.
			for _, collectionEntry := range pathEntries {
				c := collection.Collection(collectionEntry.Name())
				return traverseCollection(c, collection, filepath.Join(path, collectionEntry.Name()))
			}
		}

		// If it was neither, it's just an empty collection.
		return nil
	}

	for _, entry := range entries {
		// A Datastore may _only_ contain collections, so no files should exist at this level.
		if !entry.IsDir() {
			return datastore, ErrInvalidPath
		}

		// Create the Collection in the Datastore
		collection := datastore.Collection(entry.Name())

		// Begin traversing the collection
		if err = traverseCollection(collection, nil, filepath.Join(path, entry.Name())); err != nil {
			return datastore, err
		}
	}

	return datastore, err
}

// Close flushes the current Datastore and writes to disk. TODO it should always be writing to disk.
func (d *Datastore) Close() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Recursive function to traverse collections with collections
	var flushCollection func(name string, c *Collection) error
	flushCollection = func(name string, c *Collection) error {
		// Create the Collection directory
		collectionPath := filepath.Join(d.path, name)
		if err := os.MkdirAll(collectionPath, 0700); err != nil {
			return err
		}

		// Write all the documents in the Collection
		for documentName, document := range c.documents.Data {
			documentPath := filepath.Join(collectionPath, documentName)
			if err := os.MkdirAll(documentPath, 0700); err != nil {
				return err
			}
			for file, data := range document.data.Data {
				if err := os.WriteFile(filepath.Join(documentPath, file), []byte(base64.StdEncoding.EncodeToString(data)), 0600); err != nil {
					return err
				}
			}
		}

		// Restart the process for every Collection in the Collection
		for collectionName, collection := range c.collections.Data {
			if err := flushCollection(filepath.Join(name, collectionName), collection); err != nil {
				return err
			}
		}

		return nil
	}

	// Traverse every Collection in the Datastore
	for collectionName, collection := range d.collections.Data {
		if err := flushCollection(collectionName, collection); err != nil {
			return err
		}
	}

	return nil
}
