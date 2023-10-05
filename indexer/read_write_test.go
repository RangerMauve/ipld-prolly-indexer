package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/zeebo/assert"
	"math/rand"
	"testing"
	"time"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const number = "0123456789"

func randStr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randNumStr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = number[rand.Intn(len(number))]
	}
	return string(b)
}

func generateRandomRecord(t *testing.T, n int) ([]*studentRecord, []byte) {
	gender := []string{"male", "female"}
	records := make([]*studentRecord, 0)
	recordsBytes := make([]byte, 0)
	for i := 0; i < n; i++ {
		record := &studentRecord{
			Name:      randStr(rand.Intn(15) + 5),
			Age:       rand.Intn(6) + 12,
			Gender:    gender[rand.Intn(2)],
			School:    randStr(rand.Intn(15) + 15),
			Country:   randStr(rand.Intn(30) + 5),
			Telephone: randNumStr(15),
		}
		records = append(records, record)
		recordBytes, err := json.Marshal(record)
		assert.NoError(t, err)
		recordBytes = append(recordBytes, []byte("\n")...)
		recordsBytes = append(recordsBytes, recordBytes...)
	}

	return records, recordsBytes
}

type studentRecord struct {
	Name      string
	Age       int
	Gender    string
	School    string
	Country   string
	Telephone string
}

func Test5kRandomWriteAndReadWithoutIndex(t *testing.T) {
	records, recordsBytes := generateRandomRecord(t, 5000)

	ctx := context.Background()
	db, err := NewMemoryDatabase()
	assert.NoError(t, err)
	reader := bytes.NewReader(recordsBytes)

	collection, err := db.Collection("Students", "Name")
	assert.NoError(t, err)

	timeStart := time.Now()
	err = collection.IndexNDJSON(ctx, reader)
	assert.NoError(t, err)

	t.Logf("insert time spent: %s", time.Now().Sub(timeStart).String())

	timeStart = time.Now()
	for i := 0; i < 1000; i++ {
		query := Query{
			Equal: map[string]ipld.Node{"Name": basicnode.NewString(records[rand.Intn(5000)].Name)},
		}
		res, err := collection.Search(ctx, query)
		assert.NoError(t, err)
		<-res
	}
	t.Logf("read time spent: %s", time.Now().Sub(timeStart).String())
}

func Test5kRandomBatchWriteAndReadWithoutIndex(t *testing.T) {
	records1, recordsBytes1 := generateRandomRecord(t, 1000)
	records2, recordsBytes2 := generateRandomRecord(t, 1000)
	records3, recordsBytes3 := generateRandomRecord(t, 1000)
	records4, recordsBytes4 := generateRandomRecord(t, 1000)
	records5, recordsBytes5 := generateRandomRecord(t, 1000)

	inputs := make([][]byte, 0)
	inputs = append(inputs, recordsBytes1)
	inputs = append(inputs, recordsBytes2)
	inputs = append(inputs, recordsBytes3)
	inputs = append(inputs, recordsBytes4)
	inputs = append(inputs, recordsBytes5)

	records := append(records1, records2...)
	records = append(records, records3...)
	records = append(records, records4...)
	records = append(records, records5...)

	ctx := context.Background()
	db, err := NewMemoryDatabase()
	assert.NoError(t, err)

	collection, err := db.Collection("Students", "Name")
	assert.NoError(t, err)

	timeStart := time.Now()
	for _, input := range inputs {
		reader := bytes.NewReader(input)
		err = collection.IndexNDJSON(ctx, reader)
		assert.NoError(t, err)
	}
	t.Logf("insert time spent: %s", time.Now().Sub(timeStart).String())

	timeStart = time.Now()
	for i := 0; i < 1000; i++ {
		query := Query{
			Equal: map[string]ipld.Node{"Name": basicnode.NewString(records[rand.Intn(5000)].Name)},
		}
		res, err := collection.Search(ctx, query)
		assert.NoError(t, err)
		<-res
	}
	t.Logf("read time spent: %s", time.Now().Sub(timeStart).String())
}

func Test5kRandomWriteAndReadWithOneIndex(t *testing.T) {
	records, recordsBytes := generateRandomRecord(t, 5000)

	ctx := context.Background()
	db, err := NewMemoryDatabase()
	assert.NoError(t, err)
	reader := bytes.NewReader(recordsBytes)

	collection, err := db.Collection("Students", "Name")
	assert.NoError(t, err)

	assert.NoError(t, db.StartMutating(ctx))
	_, err = collection.CreateIndex(ctx, "Telephone")
	assert.NoError(t, err)
	assert.NoError(t, db.Flush(ctx))

	timeStart := time.Now()
	err = collection.IndexNDJSON(ctx, reader)
	assert.NoError(t, err)

	t.Logf("time spent: %s", time.Now().Sub(timeStart).String())

	timeStart = time.Now()
	for i := 0; i < 1000; i++ {
		query := Query{
			Equal: map[string]ipld.Node{"Telephone": basicnode.NewString(records[rand.Intn(5000)].Telephone)},
		}
		res, err := collection.Search(ctx, query)
		assert.NoError(t, err)
		<-res
	}
	t.Logf("read time spent: %s", time.Now().Sub(timeStart).String())
}

func Test5kRandomWriteAndReadWithTwoIndexes(t *testing.T) {
	records, recordsBytes := generateRandomRecord(t, 5000)

	ctx := context.Background()
	db, err := NewMemoryDatabase()
	assert.NoError(t, err)
	reader := bytes.NewReader(recordsBytes)

	collection, err := db.Collection("Students", "Name")
	assert.NoError(t, err)

	assert.NoError(t, db.StartMutating(ctx))
	_, err = collection.CreateIndex(ctx, "Telephone")
	assert.NoError(t, err)
	_, err = collection.CreateIndex(ctx, "School")
	assert.NoError(t, err)
	assert.NoError(t, db.Flush(ctx))

	timeStart := time.Now()
	err = collection.IndexNDJSON(ctx, reader)
	assert.NoError(t, err)

	t.Logf("time spent: %s", time.Now().Sub(timeStart).String())

	timeStart = time.Now()
	for i := 0; i < 1000; i++ {
		n := rand.Intn(5000)
		query := Query{
			Equal: map[string]ipld.Node{"Telephone": basicnode.NewString(records[n].Telephone),
				"School": basicnode.NewString(records[n].School)},
		}
		res, err := collection.Search(ctx, query)
		assert.NoError(t, err)
		<-res
	}
	t.Logf("read time spent: %s", time.Now().Sub(timeStart).String())
}

func Test5kRandomWriteAndReadWithOneIndexButTwoFields(t *testing.T) {
	records, recordsBytes := generateRandomRecord(t, 5000)

	ctx := context.Background()
	db, err := NewMemoryDatabase()
	assert.NoError(t, err)
	reader := bytes.NewReader(recordsBytes)

	collection, err := db.Collection("Students", "Name")
	assert.NoError(t, err)

	assert.NoError(t, db.StartMutating(ctx))
	_, err = collection.CreateIndex(ctx, "Telephone", "School")
	assert.NoError(t, err)
	assert.NoError(t, db.Flush(ctx))

	timeStart := time.Now()
	err = collection.IndexNDJSON(ctx, reader)
	assert.NoError(t, err)

	t.Logf("time spent: %s", time.Now().Sub(timeStart).String())

	timeStart = time.Now()
	for i := 0; i < 1000; i++ {
		n := rand.Intn(5000)
		query := Query{
			Equal: map[string]ipld.Node{"Telephone": basicnode.NewString(records[n].Telephone),
				"School": basicnode.NewString(records[n].School)},
		}
		res, err := collection.Search(ctx, query)
		assert.NoError(t, err)
		<-res
	}
	t.Logf("read time spent: %s", time.Now().Sub(timeStart).String())
}
