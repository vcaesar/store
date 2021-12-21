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
	"fmt"
	"os"
)

const (
	// DefaultStore define default store engine
	DefaultStore = "badger"
)

var supportedStore = map[string]func(path string) (Store, error){
	"ldb":    OpenLeveldb,
	"badger": OpenBadger,
	"bolt":   OpenBolt,
	// "pebble": OpenPebble,
}

// Register register the store engine
func Register(name string, fn func(path string) (Store, error)) {
	supportedStore[name] = fn
}

// Batch set db batch struct
type Batch struct {
	K, V []byte
	Kind uint8
}

// Store is store interface
type Store interface {
	Set(k, v []byte, sync ...bool) error
	Get(k []byte) ([]byte, error)
	Delete(k []byte, sync ...bool) error
	Has(k []byte) (bool, error)
	ForEach(fn func(k, v []byte) error) error
	Close() error
	WALName() string
	//
	NewBatch() error
	BatchSet(k, v []byte) error
	// BatchGet(k []byte) ([]byte, error)
	BatchDelete(k []byte) error
	Write(snyc ...bool) error
	BatchClose() error
}

// Open open the store engine
func Open(path string, args ...string) (Store, error) {
	storeName := DefaultStore

	if len(args) > 0 && args[0] != "" {
		storeName = args[0]
	} else {
		storeEnv := os.Getenv("store_engine")
		if storeEnv != "" {
			storeName = storeEnv
		}
	}

	if fn, has := supportedStore[storeName]; has {
		return fn(path)
	}

	return nil, fmt.Errorf("Unsupported store engine: %v", storeName)
}
