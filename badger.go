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
	"github.com/dgraph-io/badger/v3"
)

// Badger badger.KV db store
type Badger struct {
	db    *badger.DB
	batch *badger.WriteBatch
}

// OpenBadger open the Badger store
func OpenBadger(dbPath string) (Store, error) {
	opt := badger.DefaultOptions(dbPath)
	opt.Logger = nil

	kv, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}

	return &Badger{kv, &badger.WriteBatch{}}, err
}

// WALName is useless for this kv database
func (s *Badger) WALName() string {
	return ""
}

// Set sets the provided value for a given key.
// If key is not present, it is created. If it is present,
// the existing value is overwritten with the one provided.
//
// Use `snyc = true` set the db snyc mode
func (s *Badger) Set(k, v []byte, sync ...bool) error {
	if len(sync) > 0 {
		err := s.db.Sync()
		if err != nil {
			return err
		}
	}

	err := s.db.Update(func(txn *badger.Txn) error {
		// return txn.Set(k, v, 0x00)
		return txn.Set(k, v)
	})

	return err
}

// Get looks for key and returns a value.
// If key is not found, value is nil.
func (s *Badger) Get(k []byte) ([]byte, error) {
	var ival []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			return err
		}

		ival, err = item.ValueCopy(nil)
		return err
	})

	return ival, err
}

// Delete deletes a key. Exposing this so that user does not
// have to specify the Entry directly. For example, BitDelete
// seems internal to badger.
//
// Use `snyc = true` set the db snyc mode
func (s *Badger) Delete(k []byte, sync ...bool) error {
	if len(sync) > 0 {
		err := s.db.Sync()
		if err != nil {
			return err
		}
	}

	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})

	return err
}

// Has returns true if the DB does contains the given key.
func (s *Badger) Has(k []byte) (bool, error) {
	// return s.db.Exists(k)
	val, err := s.Get(k)
	if string(val) == "" && err != nil {
		return false, err
	}

	return true, err
}

// Len returns the size of lsm and value log files in bytes.
// It can be used to decide how often to call RunValueLogGC.
func (s *Badger) Len() (int64, int64) {
	return s.db.Size()
}

// ForEach get all key and value
func (s *Badger) ForEach(fn func(k, v []byte) error) error {
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 1000
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			if err := fn(key, val); err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

// Close closes a KV. It's crucial to call it to ensure
// all the pending updates make their way to disk.
func (s *Badger) Close() error {
	return s.db.Close()
}

// NewBatch create a db batch
func (s *Badger) NewBatch() error {
	s.batch = s.db.NewWriteBatch()
	return nil
}

// BatchSet sets the provided value for a given key with batch
func (s *Badger) BatchSet(k, v []byte) error {
	return s.batch.Set(k, v)
}

// BatchDelete deletes a key with batch
func (s *Badger) BatchDelete(k []byte) error {
	return s.batch.Delete(k)
}

// Write written the batch data to db
func (s *Badger) Write(sync ...bool) error {
	if len(sync) > 0 {
		err := s.db.Sync()
		if err != nil {
			return err
		}
	}

	return s.batch.Flush()
}

// BatchClose close the db batch
func (s *Badger) BatchClose() error {
	s.batch.Cancel()
	return nil
}
