package main

import (
"fmt"
"github.com/ipfs/go-cid"
  "github.com/ipld/go-ipld-prime"

)

struct Database {
  tree ProllyTree
  framework Framework
  collections map[string]Collection
}

struct Collection {
  db Database
  name String
  primaryKey String
}

struct Index {
  collection Collection
  fields []string
}

func IndexNDJSON(collection Collection, byteStream io.Reader) *Index {
   scanner := bufio.NewScanner(file)
    // optionally, resize scanner's capacity for lines over 64K, see next example
    for scanner.Scan() {
      line = scanner.Text()
      //Decode as ipld
      // Feed into indexer
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }
}

func (collection Collection) Indexes() []Index {
  // iterate over index list
  // get name from key
  // add getIndex
}

func (collection Collection) WriteRecord(record ipld.Node, primaryKey string) {
  // Start batch from db
  // collection prefix
  // list indexes
  // collection prefix
  // write node and get cid
  // if primary key use value from doc
  // if no pk use cid as pk
  // write cid to doc key
  // iterate over indexes
  // gen index key, insert into batch
  // flush batch
}
func (collection Collection) GetProof(key string) []cid.Cid {
  // generate key for doc id
  // get cursor for key
  // get cids up to the root
  // empty proof means it doesn't exist in the db
}

func (collection Collection) iterate() <- chan ipld.Node {
}

func main() {
  fmt.Println("Hello, World!")
}
