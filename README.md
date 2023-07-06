# ipld-prolly-indexer
Index data into queriable collections using IPLD Prolly Trees

## Features

- [ ] Import from newline delimited JSON (flat objects)
- [ ] Store as dag-cbor data
- [ ] Insert with primary key
- [ ] Search by value (all records with that value)
- [ ] Schema hints for fields
- [ ] Range search by value (numbers)
- [ ] Index creation
- [ ] Use indexes for search
- [ ] Add proofs alongside data

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

### Keyspace Subdivision

Keys are encoded as ordered byte arrays.
In the below, the character `/` represents the hex bye `0x00`.

```
// => metadata about this being an indexed prolly tree
//{collection} => metadata about collection
/{collection}/d/{cid or primary key}/ => IPLD representation of object
/{collection}/i//{name} => Metadata about index (fields/version)
/{collection}/i/{name}/{index key} => {cid or primary key}
```
