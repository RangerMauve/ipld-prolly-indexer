# ipld-prolly-indexer
Index data into queriable collections using IPLD Prolly Trees

## Features

- [x] Import from newline delimited JSON (flat objects)
- [x] Store as dag-cbor data
- [x] Insert with primary key
- [x] Search by value (all records with that value)
- [ ] Schema hints for fields
- [ ] Range search by value (numbers)
- [x] Index creation
- [x] Use indexes for search
- [x] Generate proof of inclusion
- [x] Add proofs alongside data

## Testing and Development

We use go modules, so after cloning you should run `go mod tidy` to install dependencies.

Tests are written in `indexter/indexer_test.go`.

You can run them with `go test ./...`.

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
