package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/zeebo/assert"
	"testing"

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
		fmt.Println(record.id, printer.Sprint(record.data))
	}

	err = db.ExportToFile(ctx, "fixtures/init.car")

	assert.NoError(t, err)
}

func TestSampleData(t *testing.T) {
	db, err := NewMemoryDatabase()

	assert.NoError(t, err)
	if db == nil {
		t.Fail()
	}

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

	_, err = collection.CreateIndex(ctx, "model", "created")
	assert.NoError(t, err)

	err = db.Flush(ctx)
	assert.NoError(t, err)

	err = collection.IndexNDJSON(ctx, reader)

	assert.NoError(t, err)

	err = db.ExportToFile(ctx, "fixtures/sample.car")

	assert.NoError(t, err)
}
