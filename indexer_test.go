package ipld_prolly_indexer

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/zeebo/assert"
	"testing"

	ipld "github.com/ipld/go-ipld-prime"
	datamodel "github.com/ipld/go-ipld-prime/datamodel"
	basicnode "github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/printer"
)

func TestInit(t *testing.T) {
	db, err := NewMemoryDatabase()

	assert.NoError(t, err)
	if db == nil {
		t.Fail()
	}

	reader := strings.NewReader(`{"name":"Alice"}
{"name":"Bob"}
{"name":"Albert"}
{"name":"Clearance and Steve"}`)

	// collection of users indexed by  name
	collection, err := db.Collection("users", "name")

	assert.NoError(t, err)

	ctx := context.Background()
	err = collection.IndexNDJSON(ctx, reader)
	assert.NoError(t, err)

	records, err := collection.Iterate(ctx)
	assert.NoError(t, err)

	for record := range records {
		fmt.Println(record.Id, printer.Sprint(record.Data))
	}

	err = db.ExportToFile(ctx, "fixtures/init.car")

	assert.NoError(t, err)

	query := Query{
		Equal: map[string]ipld.Node{
			"name": basicnode.NewString("Bob"),
		},
	}

	results, err := collection.Search(ctx, query)

	assert.NoError(t, err)

	record := <-results

	name, err := record.Data.LookupByString("name")

	assert.NoError(t, err)

	assert.True(t, datamodel.DeepEqual(name, basicnode.NewString("Bob")))

	proof, err := collection.GetProof(record.Id)

	assert.NoError(t, err)

	fmt.Println(proof)
}

func TestSampleData(t *testing.T) {
	db, err := NewMemoryDatabase()
	assert.NoError(t, err)
	if db == nil {
		t.Fail()
	}

	dmi, err := db.GetDBMetaInfo()
	assert.NoError(t, err)
	assert.Equal(t, dmi.Format, "database")
	assert.Equal(t, dmi.Version, DB_VERSION)

	ctx := context.Background()

	reader, err := os.Open("fixtures/sample.ndjson")
	assert.NoError(t, err)

	// collection of logs, indexed by their ID field
	collection, err := db.Collection("logs", "id")

	assert.NoError(t, err)

	err = db.StartMutating(ctx)
	assert.NoError(t, err)

	_, err = collection.CreateIndex(ctx, "created")
	assert.NoError(t, err)

	//_, err = collection.CreateIndex(ctx, "model", "created")
	//assert.NoError(t, err)

	err = db.Flush(ctx)
	assert.NoError(t, err)

	err = collection.IndexNDJSON(ctx, reader)

	assert.NoError(t, err)

	err = db.ExportToFile(ctx, "fixtures/sample.car")

	assert.NoError(t, err)

	query := Query{
		Equal: map[string]ipld.Node{
			"created": basicnode.NewInt(1688405691),
		},
	}

	expectedId := "chatcmpl-1056144062448104093141073783165392307"

	results, err := collection.Search(ctx, query)

	assert.NoError(t, err)

	record, ok := <-results

	assert.True(t, ok)

	fmt.Println(record.Id, printer.Sprint(record.Data))

	id, err := record.Data.LookupByString("id")

	assert.NoError(t, err)

	assert.True(t, datamodel.DeepEqual(id, basicnode.NewString(expectedId)))

	proof, err := collection.GetProof(record.Id)

	assert.NoError(t, err)

	fmt.Println(proof)
}
