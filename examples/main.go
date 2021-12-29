package main

import (
	"fmt"
	"log"
	"os"

	"github.com/vcaesar/store"
)

func main() {
	TestDBName := "test.db"
	db, err := store.Open(TestDBName, "badger")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("db test...")
	os.MkdirAll(TestDBName, 0777)

	err = db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		fmt.Println(err)
	}

	has, err := db.Has([]byte("key1"))
	fmt.Println(has, err)

	buf, err := db.Get([]byte("key1"))
	fmt.Println(string(buf), err)

	walFile := db.WALName()
	db.Close()
	os.Remove(walFile)
	os.RemoveAll(TestDBName)
}
