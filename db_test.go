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
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/vcaesar/tt"
)

var TestDBName = "./db_test"

func TestBadger(t *testing.T) {
	db, err := OpenBadger(TestDBName)
	tt.Expect(t, "<nil>", err)
	if err != nil {
		log.Panic(err)
	}

	log.Println("TestBadger...")
	DBTest(t, db)
	// defer db.Close()
}

func TestLdb(t *testing.T) {
	db, err := OpenLeveldb(TestDBName)
	tt.Nil(t, err)
	if err != nil {
		log.Panic(err)
	}

	log.Println("TestLdb...")
	DBTest(t, db)
	// defer db.Close()
}

func TestBolt(t *testing.T) {
	db, err := OpenBolt(TestDBName)
	tt.Nil(t, err)
	if err != nil {
		log.Panic(err)
	}

	log.Println("TestBolt...")
	DBTest(t, db)
	// defer db.Close()
}

// func TestPebble(t *testing.T) {
// 	db, err := OpenPebble(TestDBName)
// 	tt.Nil(t, err)
// 	log.Println("TestPebble...")
// 	DBTest(t, db)
// }

func DBTest(t *testing.T, db Store) {
	log.Println("db test...")
	os.MkdirAll(TestDBName, 0777)

	err := db.Set([]byte("key1"), []byte("value1"))
	tt.Nil(t, err)

	has, err := db.Has([]byte("key1"))
	tt.Equal(t, nil, err)
	if err == nil {
		tt.Equal(t, true, has)
	}

	buf := make([]byte, 100)
	buf, err = db.Get([]byte("key1"))
	tt.Nil(t, err)
	tt.Expect(t, "value1", string(buf))

	BatchTest(t, db)
	time.Sleep(1 * time.Second)

	walFile := db.WALName()
	db.Close()
	os.Remove(walFile)
	os.RemoveAll(TestDBName)
}

func BatchTest(t *testing.T, db Store) {
	err := db.NewBatch()
	tt.Nil(t, err)
	for i := 0; i < 10; i++ {
		idx := strconv.Itoa(i)
		err := db.BatchSet([]byte("k"+idx), []byte("v"+idx))
		tt.Nil(t, err)
		if i > 5 {
			err = db.BatchDelete([]byte("k" + idx))
			tt.Nil(t, err)
		}
	}
	err = db.Write()
	tt.Nil(t, err)

	val, err := db.Get([]byte("k1"))
	tt.Nil(t, err)
	tt.Equal(t, "v1", string(val))
	val, err = db.Get([]byte("k3"))
	tt.Nil(t, err)
	tt.Equal(t, "v3", string(val))

	val, err = db.Get([]byte("k7"))
	fmt.Println("Get key: ", err)
	tt.Equal(t, "[]", val)

	db.BatchClose()
}
