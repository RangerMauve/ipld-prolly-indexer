package indexer

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/RangerMauve/ipld-prolly-indexer/schema"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	sb "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"io"
	"sort"
	"strings"
	"time"

	datastore "github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"

	car2 "github.com/ipld/go-car/v2"
	carBlockstore "github.com/ipld/go-car/v2/blockstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"

	ipld "github.com/ipld/go-ipld-prime"

	dagcbor "github.com/ipld/go-ipld-prime/codec/cbor"
	dagjson "github.com/ipld/go-ipld-prime/codec/json"

	datamodel "github.com/ipld/go-ipld-prime/datamodel"
	qp "github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basicnode"

	tree "github.com/kenlabs/go-ipld-prolly-trees/pkg/tree"
)

var log = logging.Logger("ipld-prolly-indexer")

type Database struct {
	blockStore  blockstore.Blockstore
	linkSystem  *ipld.LinkSystem
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
	collection *Collection
	fields     []string
}

type op string

const (
	GreaterThan op = "GreaterThan"
	LessThan    op = "LessThan"
)

type CompareCondition struct {
	cmp       op
	indexName string
	indexVal  ipld.Node
}

func (cc *CompareCondition) Satisfy(record ipld.Node) (bool, error) {
	keyBytes, err := fieldCborBytesFromRecord(cc.indexName, record)
	if err != nil {
		return false, err
	}
	cmpRes := bytes.Compare(keyBytes, cborBytesOfNode(cc.indexVal))
	switch cc.cmp {
	case GreaterThan:
		if cmpRes > 0 {
			return true, nil
		} else {
			return false, nil
		}
	case LessThan:
		if cmpRes < 0 {
			return true, nil
		} else {
			return false, nil
		}
	default:
		return false, fmt.Errorf("unsupported compare type")
	}
}

type Record struct {
	Id   []byte
	Data ipld.Node
}

type Query struct {
	Equal   map[string]ipld.Node
	Compare *CompareCondition
	Sort    string
	Limit   int
	Skip    int
}

type InclusionProof struct {
	Key   []byte
	Proof tree.Proof
	Root  cid.Cid
}

var (
	NULL_BYTE       = []byte("\x00")
	FULL_BYTE       = []byte("\xFF")
	DATA_PREFIX     = []byte("\x00d")
	INDEX_PREFIX    = []byte("\x00i")
	DB_METADATA_KEY = []byte("\xFF\x00")
)

var (
	INDEX_VERSION_1    = int64(1)
	CURRENT_DB_VERSION = INDEX_VERSION_1
)

const (
	ChannelTimeOut = time.Second * 10
)

func FromBlockStore(blockStore blockstore.Blockstore, rootCid cid.Cid) (*Database, error) {
	nodeStore, err := tree.NewBlockNodeStore(blockStore, &tree.StoreConfig{CacheSize: 1 << 10})
	if err != nil {
		return nil, err
	}

	ptree, err := tree.LoadProllyTreeFromRootCid(rootCid, nodeStore)
	if err != nil {
		return nil, err
	}

	collections := map[string]*Collection{}

	loadedCid, err := ptree.TreeCid()
	if err != nil {
		return nil, err
	}

	if !loadedCid.Equals(rootCid) {
		return nil, errors.New("Tree CID did not load properly")
	}

	db := &Database{
		blockStore:  blockStore,
		linkSystem:  nodeStore.LinkSystem(),
		rootCid:     rootCid,
		tree:        ptree,
		collections: collections,
	}

	return db, nil
}

func NewDatabaseFromBlockStore(ctx context.Context, blockStore blockstore.Blockstore) (*Database, error) {
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
	dbMetaInfo, err := schema.BuildDBMetaInfoNode(CURRENT_DB_VERSION, "database")
	if err != nil {
		return nil, err
	}

	// Initialize the tree with metadata saying it's an indexed tree
	err = framework.Append(ctx, DB_METADATA_KEY, dbMetaInfo)
	if err != nil {
		return nil, err
	}

	ptree, rootCid, err := framework.BuildTree(ctx)
	if err != nil {
		return nil, err
	}

	db := &Database{
		blockStore,
		nodeStore.LinkSystem(),
		rootCid,
		ptree,
		collections,
	}

	return db, nil
}

func NewMemoryDatabase() (*Database, error) {
	ctx := context.Background()
	blockStore := blockstore.NewBlockstore(datastore.NewMapDatastore())

	return NewDatabaseFromBlockStore(ctx, blockStore)
}

func ImportFromFile(source string) (*Database, error) {
	blockStore, err := carBlockstore.OpenReadOnly(source, carBlockstore.UseWholeCIDs(true))

	if err != nil {
		return nil, err
	}

	roots, err := blockStore.Roots()
	if err != nil {
		return nil, err
	}

	if len(roots) == 0 {
		return nil, fmt.Errorf("No root CIDs found in CAR file")
	}

	rootCid := roots[0]

	return FromBlockStore(blockStore, rootCid)
}

// Merge two db in ProllyTree level, but the collection merging is not handling now.(i.e. maybe here exists the case
// that two collections has same name but with different primary keys)
func Merge(ctx context.Context, db *Database, other *Database) (*Database, error) {
	err := db.tree.Merge(ctx, other.tree)
	if err != nil {
		return nil, err
	}

	ch, err := other.blockStore.AllKeysChan(ctx)
	if err != nil {
		return nil, err
	}

	blksCh := make(chan blocks.Block, 0)

	var innerError error
	go func() {
		defer close(blksCh)
		for c := range ch {
			blk, err := other.blockStore.Get(ctx, c)
			if err != nil {
				innerError = err
				return
			}
			blksCh <- blk
		}
	}()

	for blk := range blksCh {
		if innerError != nil {
			return nil, innerError
		}
		err = db.blockStore.Put(ctx, blk)
		if err != nil {
			return nil, err
		}
	}

	db.rootCid = db.tree.Root

	// todo: load all collection

	// todo: handle pk and index merging
	//for name, newCol := range other.collections {
	//	if col, ok := db.collections[name]; ok {
	//		// merge exist collection
	//	} else {
	//		// add new collection
	//	}
	//}

	return db, nil
}

func (db *Database) GetDBMetaInfo() (*schema.DBMetaInfo, error) {
	res, err := db.tree.Get(DB_METADATA_KEY)
	if err != nil {
		return nil, err
	}

	dmi, err := schema.UnwrapDBMetaInfo(res)
	if err != nil {
		return nil, err
	}
	return dmi, nil
}

func (db *Database) flush(ctx context.Context) error {
	rootCid, err := db.tree.Rebuild(ctx)
	if err != nil {
		return err
	}

	db.rootCid = rootCid

	return nil
}

func (db *Database) startMutating(ctx context.Context) error {
	return db.tree.Mutate()
}

func (db *Database) ExportToFile(ctx context.Context, destination string) error {
	return car2.TraverseToFile(
		ctx,
		db.linkSystem,
		db.rootCid,
		selectorparse.CommonSelector_ExploreAllRecursively,
		destination,
	)
}

func (db *Database) saveProof(ctx context.Context, proof tree.Proof, prefix *cid.Prefix) (cid.Cid, error) {
	return db.tree.NodeStore().WriteProof(ctx, proof, prefix)
}

func (db *Database) ExportProof(ctx context.Context, prfCid cid.Cid, destination string) error {
	prf, err := db.tree.NodeStore().ReadProof(ctx, prfCid)
	if err != nil {
		return err
	}

	ssb := sb.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := ssb.ExploreRange(0, int64(len(prf)-1), ssb.ExploreRecursive(selector.RecursionLimitDepth(2), ssb.ExploreAll(ssb.ExploreRecursiveEdge())))

	return car2.TraverseToFile(
		ctx,
		db.linkSystem,
		prfCid,
		sel.Node(),
		destination,
	)
}

func (db *Database) RootCid() cid.Cid {
	return db.rootCid
}

func (db *Database) GetBlockstore() *blockstore.Blockstore {
	return &db.blockStore
}

func (db *Database) Collection(name string, primaryKey ...string) (*Collection, error) {
	if db.collections[name] == nil {
		collection := Collection{
			db,
			name,
			primaryKey,
		}
		db.collections[name] = &collection
		err := collection.Initialize()
		if err != nil {
			return nil, err
		}
	}

	return db.collections[name], nil
}

func (collection *Collection) HasPrimaryKey() bool {
	return (collection.primaryKey != nil) && (len(collection.primaryKey) != 0)
}

func (collection *Collection) Initialize() error {
	// See if there is metadata (primary key, collection version number)
	// If no metadata, take primary key and save it
	return nil
}

func (collection *Collection) IndexNDJSON(ctx context.Context, byteStream io.Reader) error {
	err := collection.db.startMutating(ctx)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(byteStream)
	for scanner.Scan() {
		line := scanner.Text()
		buffer := strings.NewReader(line)
		mapBuilder := basicnode.Prototype.Map.NewBuilder()
		err = dagjson.Decode(mapBuilder, buffer)
		if err != nil {
			return err
		}

		node := mapBuilder.Build()
		err = collection.Insert(ctx, node)
		if err != nil {
			return err
		}
	}

	err = scanner.Err()
	// For some reason it throws an EOF even when closed properly
	if err != nil && err != io.EOF {
		return err
	}

	err = collection.db.flush(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (collection *Collection) Indexes(ctx context.Context) ([]Index, error) {
	prefix := concat(collection.keyPrefix(), NULL_BYTE, INDEX_PREFIX, NULL_BYTE)
	fieldsStart := len(prefix)

	start := concat(prefix, NULL_BYTE)
	end := concat(prefix, FULL_BYTE)

	iterator, err := collection.db.tree.Search(ctx, start, end)
	if err != nil {
		return nil, err
	}

	var indexes []Index

	for !iterator.Done() {
		// Ignore the key since we don't care about it
		key, data, err := iterator.NextPair()
		if err != nil {
			return nil, err
		} else if data == nil {
			// Equivalent of done
			break
		}

		indexMetaInfo, err := schema.UnwrapIndexMetaInfo(data)
		if err != nil {
			return nil, err
		}

		if indexMetaInfo.Version != INDEX_VERSION_1 {
			return nil, fmt.Errorf("Unexpected version number in index metadata")
		}

		fields, err := ParseStringsFromCBOR(key[fieldsStart:])
		if err != nil {
			return nil, err
		}

		indexes = append(indexes, Index{
			collection,
			fields,
		})
	}

	return indexes, nil
}

func (collection *Collection) CreateIndex(ctx context.Context, fields ...string) (*Index, error) {
	err := collection.db.startMutating(ctx)
	if err != nil {
		return nil, err
	}

	index := Index{
		collection,
		fields,
	}

	if index.Exists() {
		return &index, nil
	}

	err = index.persistMetadata(ctx)
	if err != nil {
		return nil, err
	}

	err = index.Rebuild(ctx)
	if err != nil {
		return nil, err
	}

	err = collection.db.flush(ctx)
	if err != nil {
		return nil, err
	}

	return &index, nil
}

func (collection *Collection) Insert(ctx context.Context, record ipld.Node) error {
	indexes, err := collection.Indexes(ctx)
	if err != nil {
		return err
	}

	prefix := collection.keyPrefix()

	treeConfig := collection.db.tree.TreeConfig()
	linkPrefix := treeConfig.CidPrefix()
	linkProto := cidlink.LinkPrototype{Prefix: *linkPrefix}

	linkSystem := collection.db.linkSystem

	link, err := linkSystem.Store(ipld.LinkContext{Ctx: ctx}, linkProto, record)
	if err != nil {
		return err
	}

	var recordId []byte = nil
	if collection.HasPrimaryKey() {
		recordId, err = collection.recordId(record)
		if err != nil {
			return err
		}
	} else {
		recordId = []byte(link.Binary())
	}

	for _, index := range indexes {
		err = index.insert(ctx, record, recordId)
		if err != nil {
			return err
		}
	}

	recordKey := concat(prefix, DATA_PREFIX, NULL_BYTE, recordId)

	return collection.db.tree.Put(ctx, recordKey, basicnode.NewLink(link))
}

func (collection *Collection) Get(ctx context.Context, recordId []byte) (ipld.Node, error) {
	prefix := collection.keyPrefix()
	recordKey := concat(prefix, DATA_PREFIX, NULL_BYTE, recordId)

	rawNode, err := collection.db.tree.Get(recordKey)
	if err != nil {
		return nil, err
	}

	rawNodeCid, err := rawNode.AsLink()
	if err != nil {
		return nil, err
	}

	return collection.db.linkSystem.Load(
		ipld.LinkContext{Ctx: ctx},
		rawNodeCid,
		basicnode.Prototype.Map,
	)
}

func (collection *Collection) GetProof(recordId []byte) (*InclusionProof, error) {
	prefix := collection.keyPrefix()
	recordKey := concat(prefix, DATA_PREFIX, NULL_BYTE, recordId)

	proof, err := collection.db.tree.GetProof(recordKey)
	if err != nil {
		return nil, err
	}

	root := collection.db.rootCid

	lastProof := proof[len(proof)-1]

	if !root.Equals(lastProof.Node) {
		return nil, errors.New("Generated proof was invalid.")
	}

	inclusion := &InclusionProof{
		Key:   recordKey,
		Root:  root,
		Proof: proof,
	}
	return inclusion, nil
}

func (collection *Collection) recordId(record ipld.Node) ([]byte, error) {
	return indexKeyFromRecord(collection.primaryKey, record, nil)
}

func (collection *Collection) keyPrefix() []byte {
	return concat(NULL_BYTE, []byte(collection.name))
}

func (index *Index) Fields() []string {
	return index.fields
}

func (index *Index) recordKey(record ipld.Node, id []byte) ([]byte, error) {
	indexPrefix, err := index.keyPrefix()
	if err != nil {
		return nil, err
	}

	recordIndexValues, err := indexKeyFromRecord(index.fields, record, id)
	if err != nil {
		return nil, err
	}

	return concat(indexPrefix, NULL_BYTE, recordIndexValues), nil
}

func (index *Index) queryPrefix(query Query) ([]byte, error) {
	indexPrefix, err := index.keyPrefix()
	if err != nil {
		return nil, err
	}

	indexedFields := []ipld.Node{}

	for _, name := range index.fields {
		node, ok := query.Equal[name]
		if !ok {
			break
		}

		indexedFields = append(indexedFields, node)
	}

	toRemove := 0

	// Pad any missing index values with 0 bytes
	// This sets the correct "length" for the array in the key gen
	// Add an extra padding at the end for the primary key of a record
	for len(indexedFields) <= len(index.fields) {
		indexedFields = append(indexedFields, basicnode.NewInt(0))
		toRemove += 1
	}

	cborData, err := EncodeListToCBOR(indexedFields)
	if err != nil {
		return nil, err
	}

	// Remove the padded 0s
	unPadded := cborData[0 : len(cborData)-toRemove]

	finalKey := concat(indexPrefix, NULL_BYTE, unPadded)

	return finalKey, nil
}

func (index *Index) keyPrefix() ([]byte, error) {
	nameBytes, err := IndexKeyFromFields(index.fields)
	if err != nil {
		return nil, err
	}

	return concat(index.collection.keyPrefix(), INDEX_PREFIX, NULL_BYTE, nameBytes), nil
}

func (index *Index) metadataKey() ([]byte, error) {
	nameBytes, err := IndexKeyFromFields(index.fields)
	if err != nil {
		return nil, err
	}

	return concat(index.collection.keyPrefix(), NULL_BYTE, INDEX_PREFIX, NULL_BYTE, nameBytes), nil
}

func (index *Index) Exists() bool {
	key, err := index.metadataKey()
	if err != nil {
		return false
	}

	// check existence
	n, err := index.collection.db.tree.Get(key)
	if err != nil {
		return false
	}
	// check type
	_, err = schema.UnwrapIndexMetaInfo(n)
	return err == nil
}

func (index *Index) Rebuild(ctx context.Context) error {
	// Iterate over records in collection
	// Insert index keys for each one
	return nil
}

func (index *Index) insert(ctx context.Context, record ipld.Node, recordId []byte) error {
	indexRecordKey, err := index.recordKey(record, recordId)
	if err != nil {
		return err
	}

	ipldRecordId := basicnode.NewBytes(recordId)

	return index.collection.db.tree.Put(ctx, indexRecordKey, ipldRecordId)
}

func (index *Index) persistMetadata(ctx context.Context) error {
	key, err := index.metadataKey()
	if err != nil {
		return err
	}

	metadata, err := schema.BuildIndexMetaInfoNode(INDEX_VERSION_1)
	if err != nil {
		return err
	}

	return index.collection.db.tree.Put(ctx, key, metadata)
}

func (collection *Collection) Iterate(ctx context.Context) (<-chan Record, error) {
	prefix := collection.keyPrefix()
	idStart := len(prefix) + len(DATA_PREFIX) + len(NULL_BYTE)
	start := concat(prefix, DATA_PREFIX, NULL_BYTE)
	end := concat(prefix, DATA_PREFIX, FULL_BYTE)

	iterator, err := collection.db.tree.Search(ctx, start, end)
	if err != nil {
		return nil, err
	}

	linkSystem := collection.db.linkSystem

	c := make(chan Record)

	// TODO: Better error handling
	go func(ch chan<- Record) {
		defer close(ch)
	IteratorLoop:
		for !iterator.Done() {
			// Ignore the key since we don't care about it
			key, rawNode, err := iterator.NextPair()
			if err != nil {
				panic(err)
			} else if rawNode == nil {
				break
			}

			rawNodeCid, err := rawNode.AsLink()
			if err != nil {
				panic(err)
			}

			data, err := linkSystem.Load(
				ipld.LinkContext{Ctx: ctx},
				rawNodeCid,
				basicnode.Prototype.Map,
			)
			if err != nil {
				panic(err)
			}

			id := key[idStart:]

			record := Record{
				Id:   id,
				Data: data,
			}

			// TODO: What about the error?
			select {
			case <-ctx.Done():
				break IteratorLoop
			case <-time.After(ChannelTimeOut):
				break IteratorLoop
			case ch <- record:
			}
		}
	}(c)

	return c, nil
}

//func (collection *Collection) _Search(ctx context.Context, query Query) (<-chan Record, error) {
//	index, err := collection.BestIndex(ctx, query)
//	if err != nil {
//		return nil, err
//	}
//
//	count := 0
//	skipped := 0
//
//	if index == nil {
//		// Iterate all and filter as you go
//		all, err := collection.Iterate(ctx)
//		if err != nil {
//			return nil, err
//		}
//
//		c := make(chan Record)
//
//		// TODO: Better error handling
//		go func(ch chan<- Record) {
//			defer close(ch)
//
//		IteratorLoop:
//			for record := range all {
//				if query.Matches(record) {
//					if skipped < query.Skip {
//						skipped++
//						continue
//					}
//					if query.Limit != 0 {
//						if count >= query.Limit {
//							break
//						} else {
//							count++
//						}
//					}
//					select {
//					case <-ctx.Done():
//						break IteratorLoop
//					case <-time.After(ChannelTimeOut):
//						break IteratorLoop
//					case ch <- record:
//					}
//				}
//			}
//		}(c)
//
//		return c, nil
//	} else {
//		// Get fields
//		start, err := index.queryPrefix(query)
//		if err != nil {
//			return nil, err
//		}
//		end := make([]byte, len(start))
//		copy(end, start)
//		// TODO: Detect if ending is already 0xFF and keep going back filling up
//		end[len(end)-1] = 0xFF
//
//		iterator, err := collection.db.tree.Search(ctx, start, end)
//		if err != nil {
//			return nil, err
//		}
//
//		c := make(chan Record)
//
//		// TODO: Better error handling
//		go func(ch chan<- Record) {
//			defer close(ch)
//		IteratorLoop:
//			for !iterator.Done() {
//				// Ignore the key since we don't care about it
//				_, recordIdNode, err := iterator.NextPair()
//
//				if recordIdNode == nil && err == nil {
//					break
//				}
//
//				if err != nil {
//					panic(err)
//				}
//
//				recordId, err := recordIdNode.AsBytes()
//
//				if err != nil {
//					panic(err)
//				}
//
//				// TODO: Pull additional fields from query key before loading record
//				data, err := collection.Get(ctx, recordId)
//				if err != nil {
//					panic(err)
//				}
//
//				record := Record{
//					Id:   recordId,
//					Data: data,
//				}
//
//				if !query.Matches(record) {
//					continue
//				}
//
//				if skipped < query.Skip {
//					skipped++
//					continue
//				}
//				if query.Limit != 0 {
//					if count >= query.Limit {
//						break
//					} else {
//						count++
//					}
//				}
//
//				// TODO: What about the error?
//				select {
//				case <-ctx.Done():
//					break IteratorLoop
//				case <-time.After(ChannelTimeOut):
//					break IteratorLoop
//				case ch <- record:
//				}
//			}
//		}(c)
//
//		return c, nil
//	}
//}

func (collection *Collection) Search(ctx context.Context, query Query) (<-chan Record, error) {
	index, err := collection.BestIndex(ctx, query)
	if err != nil {
		return nil, err
	}

	count := 0
	skipped := 0

	c := make(chan Record)
	if index == nil {
		// Iterate all and filter as you go
		all, err := collection.Iterate(ctx)
		if err != nil {
			return nil, err
		}

		// TODO: Better error handling
		go func(ch chan<- Record) {
			defer close(ch)

		IteratorLoop:
			for record := range all {
				if query.Matches(record) {
					if skipped < query.Skip {
						skipped++
						continue
					}
					if query.Limit != 0 {
						if count >= query.Limit {
							break
						} else {
							count++
						}
					}
					select {
					case <-ctx.Done():
						break IteratorLoop
					case <-time.After(ChannelTimeOut):
						break IteratorLoop
					case ch <- record:
					}
				}
			}
		}(c)

	} else {
		// Get fields
		start, err := index.queryPrefix(query)
		if err != nil {
			return nil, err
		}
		end := make([]byte, len(start))
		copy(end, start)
		// TODO: Detect if ending is already 0xFF and keep going back filling up
		end[len(end)-1] = 0xFF

		iterator, err := collection.db.tree.Search(ctx, start, end)
		if err != nil {
			return nil, err
		}

		// TODO: Better error handling
		go func(ch chan<- Record) {
			defer close(ch)
		IteratorLoop:
			for !iterator.Done() {
				// Ignore the key since we don't care about it
				_, recordIdNode, err := iterator.NextPair()

				if recordIdNode == nil && err == nil {
					break
				}

				if err != nil {
					panic(err)
				}

				recordId, err := recordIdNode.AsBytes()

				if err != nil {
					panic(err)
				}

				// TODO: Pull additional fields from query key before loading record
				data, err := collection.Get(ctx, recordId)
				if err != nil {
					panic(err)
				}

				record := Record{
					Id:   recordId,
					Data: data,
				}

				if !query.Matches(record) {
					continue
				}

				if skipped < query.Skip {
					skipped++
					continue
				}
				if query.Limit != 0 {
					if count >= query.Limit {
						break
					} else {
						count++
					}
				}

				// TODO: What about the error?
				select {
				case <-ctx.Done():
					break IteratorLoop
				case <-time.After(ChannelTimeOut):
					break IteratorLoop
				case ch <- record:
				}
			}
		}(c)
	}

	if query.Sort != "" {
		res := make(chan Record)
		records := make([]Record, 0)
		for record := range c {
			records = append(records, record)
		}
		var innerError error
		sort.Slice(records, func(i, j int) bool {
			keyi, err := fieldCborBytesFromRecord(query.Sort, records[i].Data)
			if err != nil {
				innerError = err
				return false
			}
			keyj, err := fieldCborBytesFromRecord(query.Sort, records[j].Data)
			if err != nil {
				innerError = err
				return false
			}
			if bytes.Compare(keyi, keyj) <= 0 {
				return true
			} else {
				return false
			}
		})
		if innerError != nil {
			return nil, innerError
		}
		go func() {
			for _, record := range records {
				res <- record
			}
			close(res)
		}()
		return res, nil
	} else {
		return c, nil
	}
}

func (collection *Collection) BestIndex(ctx context.Context, query Query) (*Index, error) {
	indexes, err := collection.Indexes(ctx)

	if err != nil {
		return nil, err
	}

	var best *Index
	bestMatchingFields := 0

	for _, index := range indexes {
		// Iterate over the fields
		matchingFields := 0
		for _, field := range index.fields {
			_, ok := query.Equal[field]
			if ok {
				matchingFields += 1
			}
		}
		if matchingFields == 0 {
			continue
		}
		if matchingFields > bestMatchingFields {
			best = &index
			bestMatchingFields = matchingFields
		} else if matchingFields == bestMatchingFields {
			if len(best.fields) > len(index.fields) {
				best = &index
			}
		}
	}

	return best, nil
}

func (query Query) Matches(record Record) bool {
	for field, expected := range query.Equal {
		value, err := record.Data.LookupByString(field)

		if err != nil || value == nil {
			return false
		}

		if datamodel.DeepEqual(value, expected) != true {
			return false
		}
	}

	if query.Compare != nil {
		if ok, _ := query.Compare.Satisfy(record.Data); !ok {
			return false
		}
	}

	return true
}

func (record *Record) AsIPLD() (ipld.Node, error) {
	return qp.BuildMap(basicnode.Prototype.Any, -1, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "Id", qp.Bytes(record.Id))
		qp.MapEntry(ma, "Data", qp.Node(record.Data))
	})
}

func (record *Record) AsIPLDWithProof(proof tree.Proof) (ipld.Node, error) {
	recordNode, err := record.AsIPLD()
	if err != nil {
		return nil, err
	}
	ProofNode, err := proof.ToNode()
	if err != nil {
		return nil, err
	}
	return qp.BuildMap(basicnode.Prototype.Any, -1, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "Record", qp.Node(recordNode))
		qp.MapEntry(ma, "Proof", qp.Node(ProofNode))
	})
}

func ParseStringsFromCBOR(data []byte) ([]string, error) {
	ipldList, err := ParseListFromCBOR(data)

	if err != nil {
		return nil, err
	}

	var items []string

	iterator := ipldList.ListIterator()

	for !iterator.Done() {
		_, value, err := iterator.Next()
		if err != nil {
			return nil, err
		}

		asString, err := value.AsString()
		if err != nil {
			return nil, err
		}

		items = append(items, asString)

	}

	return items, nil
}

func ParseListFromCBOR(data []byte) (ipld.Node, error) {
	builder := basicnode.Prototype.List.NewBuilder()
	reader := bytes.NewReader(data)
	err := dagcbor.Decode(builder, reader)
	if err != nil {
		return nil, err
	}

	return builder.Build(), nil
}

func EncodeListToCBOR(data []ipld.Node) ([]byte, error) {
	assembleKeyNode := func(am datamodel.ListAssembler) {
		for _, value := range data {
			qp.ListEntry(am, qp.Node(value))
		}
	}

	keyNode, err := qp.BuildList(basicnode.Prototype.Any, -1, assembleKeyNode)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	err = dagcbor.Encode(keyNode, &buf)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func IndexKeyFromFields(fields []string) ([]byte, error) {
	assembleKeyNode := func(am datamodel.ListAssembler) {
		for _, key := range fields {
			qp.ListEntry(am, qp.String(key))
		}
	}

	keyNode, err := qp.BuildList(basicnode.Prototype.Any, -1, assembleKeyNode)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	err = dagcbor.Encode(keyNode, &buf)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func fieldCborBytesFromRecord(key string, record ipld.Node) ([]byte, error) {
	value, err := record.LookupByString(key)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = dagcbor.Encode(value, &buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func cborBytesOfNode(n ipld.Node) []byte {
	var buf bytes.Buffer
	err := dagcbor.Encode(n, &buf)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func indexKeyFromRecord(keys []string, record ipld.Node, id []byte) ([]byte, error) {
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
	if err != nil {
		return nil, err
	}
	if hadError != nil {
		return nil, hadError
	}

	var buf bytes.Buffer

	err = dagcbor.Encode(keyNode, &buf)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func concat(buffers ...[]byte) []byte {
	fullSize := 0
	for _, buf := range buffers {
		fullSize = fullSize + len(buf)
	}

	final := make([]byte, fullSize)

	offset := 0
	for _, buf := range buffers {
		buffLen := len(buf)
		copy(final[offset:offset+buffLen], buf)
		offset = offset + buffLen
	}

	return final
}
