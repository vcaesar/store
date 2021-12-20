// Copyright 2016 ego authors
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package store

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// Leveldb leveldb store struct
type Leveldb struct {
	db    *leveldb.DB
	batch *leveldb.Batch
}

// OpenLeveldb opens or creates a DB for the given store.
// The DB will be created if not exist, unless ErrorIfMissing is true.
func OpenLeveldb(dbPath string) (Store, error) {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return nil, err
	}

	return &Leveldb{db, &leveldb.Batch{}}, nil
}

// WALName is useless for this kv database
func (s *Leveldb) WALName() string {
	return "" // not use with this db
}

// Set sets the provided value for a given key.
// If key is not present, it is created.
func (s *Leveldb) Set(k, v []byte, sync ...bool) error {
	var wo opt.WriteOptions
	if len(sync) > 0 {
		wo = opt.WriteOptions{Sync: true}
	}
	return s.db.Put(k, v, &wo)
}

// Get gets the value for the given key. It returns
// ErrNotFound if the DB does not contains the key.
func (s *Leveldb) Get(k []byte) ([]byte, error) {
	if len(k) == 0 {
		return nil, nil
	}
	return s.db.Get(k, nil)
}

// Delete deletes the value for the given key.
// Delete will not returns error if key doesn't exist.
// Write merge also applies for Delete, see Write.
func (s *Leveldb) Delete(k []byte, sync ...bool) error {
	var wo opt.WriteOptions
	if len(sync) > 0 {
		wo = opt.WriteOptions{Sync: true}
	}
	return s.db.Delete(k, &wo)
}

// Has returns true if the DB does contains the given key.
// It is safe to modify the contents of the argument after Has returns.
func (s *Leveldb) Has(k []byte) (bool, error) {
	return s.db.Has(k, nil)
}

// Len calculates approximate sizes of the given key ranges.
// The length of the returned sizes are equal with the length of
// the given ranges.
func (s *Leveldb) Len() (leveldb.Sizes, error) {
	return s.db.SizeOf(nil)
}

// ForEach get all key and value
func (s *Leveldb) ForEach(fn func(k, v []byte) error) error {
	iter := s.db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		val := iter.Value()
		if err := fn(key, val); err != nil {
			return err
		}
	}

	iter.Release()
	return iter.Error()
}

// Close closes the DB. This will also releases any outstanding snapshot,
// abort any in-flight compaction and discard open transaction.
func (s *Leveldb) Close() error {
	return s.db.Close()
}
