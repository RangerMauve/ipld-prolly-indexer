package schema

import (
	_ "embed"
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

var (
	DBMetaInfoPrototype    schema.TypedPrototype
	IndexMetaInfoPrototype schema.TypedPrototype

	//go:embed schema.ipldsch
	schemaBytes []byte
)

func init() {
	typeSystem, err := ipld.LoadSchemaBytes(schemaBytes)
	if err != nil {
		panic(fmt.Errorf("failed to load schema: %w", err))
	}
	DBMetaInfoPrototype = bindnode.Prototype((*DBMetaInfo)(nil), typeSystem.TypeByName("DBMetaInfo"))
	IndexMetaInfoPrototype = bindnode.Prototype((*IndexMetaInfo)(nil), typeSystem.TypeByName("IndexMetaInfo"))
}
