package main

import (
	"context"
	"fmt"
	"github.com/zeebo/assert"
	"strings"
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
		fmt.Println(printer.Sprint(record))
	}

	err = db.ExportToFile(ctx, "fixtures/init.car")

	assert.NoError(t, err)

	fmt.Println("Finish")
}
