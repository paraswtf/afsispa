// check_hash.go
package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"

	"github.com/dgraph-io/badger/v3"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: go run check_hash.go <16-hex-hash> [dbpath]")
		os.Exit(2)
	}
	hstr := os.Args[1]
	dbPath := "./indexdb"
	if len(os.Args) >= 3 {
		dbPath = os.Args[2]
	}

	b, err := hex.DecodeString(hstr)
	if err != nil {
		log.Fatalf("hex decode: %v", err)
	}
	if len(b) != 8 {
		log.Fatalf("expected 8 bytes (16 hex chars); got %d bytes", len(b))
	}
	key := make([]byte, 8)
	copy(key, b)

	db, err := badger.Open(badger.DefaultOptions(dbPath).WithLogger(nil))
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			n := len(v)
			if n > 500 {
				n = 500
			}
			fmt.Printf("value preview (first %d bytes):\n%s\n", n, string(v[:n]))
			return nil
		})
	})
	if err != nil {
		fmt.Println("not found or error:", err)
	} else {
		fmt.Println("key found")
	}
}