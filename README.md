# ipld-prolly-indexer
Index data into queriable collections using IPLD Prolly Trees

## Features

- [x] Import from newline delimited JSON (flat objects)
- [x] Store as dag-cbor data
- [x] Insert with primary key
- [x] Search by value (all records with that value)
- [x] Index creation
- [x] Use indexes for search
- [x] Generate proof of inclusion
- [x] Add proofs alongside data

## Future Features

- [ ] Schema hints for fields
- [ ] Range search by value (numbers)

## Usage

```Golang
import (
	"context"
	"fmt"
	"github.com/RangerMauve/ipld-prolly-indexer/indexer"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/printer"
	"strings"
)

func main() {
	ctx := context.Background()
	// create memory db
	db, err := indexer.NewMemoryDatabase()
	if err != nil {
		panic(err)
	}

	// input data
	reader := strings.NewReader(`{"name":"Alice", "age": 18}
					{"name":"Bob", "age": 19}
					{"name":"Albert", "age": 20}
					{"name":"Clearance and Steve", "age":18}`)

	// create collection and set primary key
	collection, err := db.Collection("users", "name")
	if err != nil {
		panic(err)
	}

	// create index in age field
	_, err = collection.CreateIndex(ctx, "age")
	if err != nil {
		panic(err)
	}

	// insert data
	err = collection.IndexNDJSON(ctx, reader)
	if err != nil {
		panic(err)
	}

	// iterate all records and print
	records, err := collection.Iterate(ctx)
	if err != nil {
		panic(err)
	}
	for record := range records {
		fmt.Println(printer.Sprint(record.Data))
	}

	// export all data of db as car file
	err = db.ExportToFile(ctx, "./init.car")
	if err != nil {
		panic(err)
	}

	// query record
	query := indexer.Query{
		Equal: map[string]ipld.Node{
			"name": basicnode.NewString("Bob"),
		},
	}
	results, err := collection.Search(ctx, query)
	if err != nil {
		panic(err)
	}

	record := <-results
	fmt.Println(printer.Sprint(record.Data))

	// get proof of record(ProllyTree path)
	proof, err := collection.GetProof(record.Id)
	if err != nil {
		panic(err)
	}

	fmt.Println(proof)

	// query record with index field
	query = indexer.Query{
		Equal: map[string]ipld.Node{
			"age": basicnode.NewInt(18),
		},
	}
	results, err = collection.Search(ctx, query)
	if err != nil {
		panic(err)
	}

	// two records
	for record = range results {
		fmt.Println(printer.Sprint(record.Data))
	}
}
```

## API
### `func NewDatabaseFromBlockStore(ctx context.Context, blockStore blockstore.Blockstore) (*Database, error)`
Initialize a new database from blockstore.

### `func (db *Database) Collection(name string, primaryKey ...string) (*Collection, error)`
Manually creates a collection with the name and primary key fields or return the existed collection.

### `func (collection *Collection) IndexNDJSON(ctx context.Context, byteStream io.Reader) error`
Write ndjson data into the collection, each line in ndjson is regarded as a record, if here exist some indexes, the indexes will be inserted as well.

### `func (collection *Collection) Iterate(ctx context.Context) (<-chan Record, error)`
Iterate all records in the collection, it is not recommanded.

### `func (db *Database) ExportToFile(ctx context.Context, destination string) error`
Export all data of DB into the car file.

### `func ImportFromFile(source string) (*Database, error)`
Load database from the car file.

### `func Merge(ctx context.Context, db *Database, other *Database) (*Database, error)`
Merge two DBs includes ProllyTree and raw data in blockstore.
### `func (collection *Collection) Search(ctx context.Context, query Query) (<-chan Record, error)`
Search records using the query.Return the channel that closes while end

### `func (collection *Collection) CreateIndex(ctx context.Context, fields ...string) (*Index, error)`
Create an index for a set of fields.This will go over all the documents inserted after the index created in the collection and if they have the apropriate fields, will add them to the index.Indexing fields is important if you want to sort based on a query, or want to take advantage of sparsely querying the dataset to speed things up.

### `func (collection *Collection) GetProof(recordId []byte) (*InclusionProof, error)`
Get proof of a record in ProllyTree by its recordId, it includes recordKey, prollytree root cid and the path that recordKey in the tree.

### `func (db *Database) ExportProof(ctx context.Context, prfCid cid.Cid, destination string) error`
Export the proof of a record and related blocks

### `func (collection *Collection) Get(ctx context.Context, recordId []byte) (ipld.Node, error)`
Get data by the recordId

### `func (collection *Collection) Indexes(ctx context.Context) ([]Index, error)`
Return all indexes of the collection

### `func (collection *Collection) Insert(ctx context.Context, record ipld.Node) error`
Insert a record into DB including raw data and index creating

### `func (db *Database) GetDBMetaInfo() (*schema.DBMetaInfo, error)`
Get metainfo about the database

### `func (record *Record) AsIPLDWithProof(proof tree.Proof) (ipld.Node, error)`
Encode record and its proof as ipld node.

### `func (db *Database) RootCid() cid.Cid`
Return current ProllyTree root cid. It will be updated after ProllyTree modified.

### `func (db *Database) GetBlockstore() *blockstore.Blockstore`
Get the base blockstore that store the ProllyTree and the raw records

### `func (index *Index) Fields() []string`
Return the fields of the index, the order is fixed.

### `func (index *Index) Exists() bool`
Return whether the index exists in the DB

### `func (collection *Collection) BestIndex(ctx context.Context, query Query) (*Index, error)`
Choose best index for a query

### `func (query Query) Matches(record Record) bool`
Return whether the record matches the query or not



## Testing and Development

We use go modules, so after cloning you should run `go mod tidy` to install dependencies.

Tests are written in `indexter/indexer_test.go`.

You can run them with `go test ./...`.

## Benchmark

You can run `./indexer/read_write_test.go` to run the base benchmark test in your computer. The result is below.


PC: MacbookPro M1, OS: Macos, RAM: 16G

| Test Condition  | Write records number  | Read records number  | Spent time（Write/Read）  |
|:----------|:----------|:----------|:----------|
| Without Index | 5000    | 1000    | 120.61ms/9.41s    |
| 5 batches Writing and without index    | 1000 * 5    | 1000    | 249.02ms/11.26s    |
| One index  | 5000    | 1000   | 191.79ms/14.86ms    |
| Two indexes   | 5000    | 1000    | 264.98ms/16.85ms    |
| One index with two fields    | 5000    | 1000    | 190.29ms/14.05ms    |


## Spec

### Overview

- Data is stored in IPLD Prolly Trees
- Keyspace is subdivided to store collections of records
- Collections have raw documents and indexes over those documents
- documents are referenced either by primary key (a specific field) orthe immutable CID
- indexes encode fields as binary data for fast sort and facilitating ranged queries

Loosely based on [hyperbeedeebee](https://github.com/RangerMauve/hyperbeedeebee)

### Keyspace Encoding

### Index key creation

- take document `doc`
- take primary key `pk`
- take indexed fields `fields`
- make array `key`
- for each `field` in `fields`, append `field` to `key`
- append `pk` to key
- encode `key` and return bytes

### Record insert

- get all indexes of the collection
- get collection keyPrefix
- store the record(ipld node) in LinkSystem and get cid(link)
- generate recordId
	- if collection has primary key, return indexKeyFromRecord(pk, record)
	- else return cid bytes as recordId
- for each index in collection, index.insert(ctx, record, recordId)
	- get indexRecordKey from index.recordKey(record, recordId)
	- Put (indexRecordKey, recordId) in ProllyTree
- get recordKey from concat(keyPrefix, DATA_PREFIX, NULL_BYTE, recordId)
- Put (recordKey, link)

### Keyspace Subdivision

Keys are encoded as ordered byte arrays.

#### `\xFF\x00`

Metadata storage. Contains an IPLD Map.
`"version"` integer property set to `1` representing the DB version. Breaking changes to the database representation must have incremented versions.
`"type"` string property set to "database" to signify this is an indexed database within the prolly tree.

#### `\x00{collection utf8 name}\x00\x00`

TODO: Collection metadata with primary key

#### `\x00{collection utf8 name}\x00d\x00{primary key bytes}`

Storage of individual record within the DB.
Points to a CID for the IPLD representation of the database record.
Records must be encoded using the same codec and hash options as the rest of the prolly tree.
This CID can be used to resolve to the raw record data.

The collection must have a utf8 encoded "name" which uniquely identifies it's data in the DB

The primary key will consist of either a dag-cbor array of the uniquly identifiying properties from the record, or the CID of the record as bytes.

#### `\x00{collection utf8 name}\x00\x00\i\x00{dag-cbor array of field names}`

Index Metadata storage. Contains an IPLD Map.
`"version"` integer property set to `1` representing the Index creation version.
This field will be increased for new index types and versions.
Additional properties may be added in the future.

The fields being indexed are represented in a dag-cbor array and this is used as the "name" of the index.

#### `\x00{collection utf8 name}\x00i\x00{dag-cbor array of field names}\x00{index key bytes}`

Indexed record keys. The fields from the record will be pulled out and turned into a dag-cbor array containing the fields, and the record ID.

This will point to a value of the primary key for the record.

The primary key can then be used to resolve the record.

## Data Struct

```Golang
type Database struct {
	blockStore blockstore.Blockstore
	linkSystem  *ipld.LinkSystem
	rootCid     cid.Cid
	tree        *tree.ProllyTree
	collections map[string]*Collection
}
```
 linkSystem and prollyTree are based on the blockstore, root cid is included in tree.


```Golang
type Collection struct {
	db         *Database
	name       string
	primaryKey []string
}
```

```Golang
type Index struct {
	collection *Collection
	fields     []string
}
```

```Golang
type Query struct {
	Equal   map[string]ipld.Node
	Compare *CompareCondition
	Sort    string
	Limit   int
	Skip    int
}
```
Query is used in Search() api.

If there are indexed fields in the equal map and no sort field, search will choose the best index(most consecutive from the start of the index based on the fields existed in equal map). 

If the sort field is not empty, the consecutive must end with the sort field but the fields after the sort filed are ignored.  

If the equal map is nil or there is no qualified index, search will iterate all records and filter out records that don't match the query. 


```Golang
type InclusionProof struct {
	Key   []byte
	Proof tree.Proof
	Root  cid.Cid
}
```

Proof is the ProllyTree path of the record, root cid is cid of ProllyTree's root.It should be noted that you may get different InclusionProofs for a record in different time because the ProllyTree may be updated.
