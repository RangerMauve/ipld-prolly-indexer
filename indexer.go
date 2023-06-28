package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/ipfs/go-cid"
	"io"
	"strings"

	datastore "github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipld/go-ipld-prime"

	dagcbor "github.com/ipld/go-ipld-prime/codec/cbor"
	dagjson "github.com/ipld/go-ipld-prime/codec/json"

	datamodel "github.com/ipld/go-ipld-prime/datamodel"
	qp "github.com/ipld/go-ipld-prime/fluent/qp"
	basicnode "github.com/ipld/go-ipld-prime/node/basicnode"

	tree "github.com/kenlabs/go-ipld-prolly-trees/pkg/tree"
)

type Database struct {
	blockStore  *blockstore.Blockstore
	nodeStore   *tree.BlockNodeStore
	rootCid     cid.Cid
	tree        *tree.ProllyTree
	collections map[string]*Collection
}

type Collection struct {
	db         *Database
	name       string
	primaryKey []string
}

type Index struct {
	collection Collection
	fields     []string
}

var NULL_BYTE = []byte("\x00")
var FULL_BYTE = []byte("\xFF")
var DATA_PREFIX = []byte("\x00d")

func NewDatabase() (*Database, error) {
	ctx := context.Background()
	blockStore := blockstore.NewBlockstore(datastore.NewMapDatastore())
	nodeStore, err := tree.NewBlockNodeStore(blockStore, &tree.StoreConfig{CacheSize: 1 << 10})

	if err != nil {
		return nil, err
	}

	// We're going to construct a fresh ProllyTree as our root to apply changes on
	chunkConfig := tree.DefaultChunkConfig()
	framework, err := tree.NewFramework(ctx, nodeStore, chunkConfig, nil)

	if err != nil {
		return nil, err
	}

	collections := map[string]*Collection{}

	tree, rootCid, err := framework.BuildTree(ctx)

	if err != nil {
		return nil, err
	}

	db := &Database{
		&blockStore,
		nodeStore,
		rootCid,
		tree,
		collections,
	}

	return db, nil
}

func (db Database) Flush(ctx context.Context) error {
	rootCid, err := db.tree.Rebuild(ctx)

	if err != nil {
		return err
	}

	db.rootCid = rootCid

	return nil
}

func (db Database) Collection(name string, primaryKey ...string) (*Collection, error) {
	if db.collections[name] == nil {
		collection := Collection{
			&db,
			name,
			primaryKey,
		}
		db.collections[name] = &collection
		collection.Initialize()
	}

	return db.collections[name], nil
}

func (collection Collection) HasPrimaryKey() bool {
	return len(collection.primaryKey) != 0
}

func (collection Collection) Initialize() error {
	// See if there is metadata (primary key, collection version number)
	// If no metadata, take primary key and save it
	return nil
}

func (collection Collection) IndexNDJSON(ctx context.Context, byteStream io.Reader) error {
	scanner := bufio.NewScanner(byteStream)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	for scanner.Scan() {
		line := scanner.Text()
		buffer := strings.NewReader(line)
		mapBuilder := basicnode.Prototype.Map.NewBuilder()
		err := dagjson.Decode(mapBuilder, buffer)

		if err != nil {
			return err
		}

		node := mapBuilder.Build()
		writeErr := collection.WriteRecord(ctx, node)

		if writeErr != nil {
			return err
		}
	}

	err := scanner.Err()
	if err != nil {
		return err
	}

	flushErr := collection.db.Flush(ctx)

	if flushErr != nil {
		return flushErr
	}

	return nil
}

func (collection Collection) Indexes() []Index {
	// iterate over index list
	// get name from key
	// add getIndex
	return nil
}

func (collection Collection) WriteRecord(ctx context.Context, record ipld.Node) error {
	prefix := collection.keyPrefix()

	recordId, err := collection.recordId(record)

	if err != nil {
		return err
	}

	recordKey := Concat(prefix, DATA_PREFIX, NULL_BYTE, recordId)

	return collection.db.tree.Put(ctx, recordKey, record)

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
}

func (collection Collection) GetProof(key string) []cid.Cid {
	// generate key for doc id
	// get cursor for key
	// get cids up to the root
	// empty proof means it doesn't exist in the db
	return nil
}

func (collection Collection) recordId(record ipld.Node) ([]byte, error) {
	return IndexKeyFromRecord(collection.primaryKey, record, nil)
}

func (collection Collection) keyPrefix() []byte {
	return Concat(NULL_BYTE, []byte(collection.name))
}

func (collection Collection) iterate(ctx context.Context) (<-chan ipld.Node, error) {
	prefix := collection.keyPrefix()
	start := Concat(prefix, DATA_PREFIX, NULL_BYTE)
	end := Concat(prefix, DATA_PREFIX, FULL_BYTE)

	iterator, err := collection.db.tree.Search(ctx, start, end)

	if err != nil {
		return nil, err
	}

	c := make(chan ipld.Node)

	go func(ch chan<- ipld.Node) {
		for !iterator.Done() {
			// Ignore the key since we don't care about it
			_, value, err := iterator.NextPair()

			if err != nil {
				panic(err)
			}

			// TODO: What about the error?
			c <- value
		}
	}(c)

	return c, nil
}

func IndexKeyFromRecord(keys []string, record ipld.Node, id []byte) ([]byte, error) {
	var hadError error
	assembleKeyNode := func(am datamodel.ListAssembler) {
		for _, key := range keys {
			value, err := record.LookupByString(key)
			if err != nil {
				hadError = err
				break
			}
			qp.ListEntry(am, qp.Node(value))
		}
		if id != nil && len(id) != 0 {
			qp.ListEntry(am, qp.Bytes(id))
		}
	}

	keyNode, err := qp.BuildList(basicnode.Prototype.Any, int64(len(keys)), assembleKeyNode)

	if hadError != nil {
		return nil, hadError
	}

	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	encodeErr := dagcbor.Encode(keyNode, &buf)

	if encodeErr != nil {
		return nil, encodeErr
	}

	return buf.Bytes(), nil
}

func Concat(buffers ...[]byte) []byte {
	fullSize := 0
	for _, buf := range buffers {
		fullSize = fullSize + len(buf)
	}

	final := make([]byte, 0, fullSize)

	for _, buf := range buffers {
		final = append(final, buf...)
	}

	return final
}

func main() {
	fmt.Println("Hello, World!")
}
