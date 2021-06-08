package graphql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	graphql "github.com/graph-gophers/graphql-go"
)

// Resolver is the root resolver
// it can hold some attribute to resolve our requests, e.g.
// a database pointer which we can use to execute the queries
type Resolver struct {
	db *sql.DB
}

// GetDataset resolves the getDataset query
func (r *Resolver) GetDataset(ctx context.Context, args struct{ Name string }) (*DatasetResolver, error) {

	// get dataset info, e.g. from underlying DB
	dataset := Dataset{Name: "/a/b/RAW"}
	//     var dataset Dataset
	dr := DatasetResolver{
		d: dataset,
	}

	return &dr, nil
}

// AddDataset implements addDataset of graphql schema
func (r *Resolver) AddDataset(ctx context.Context, args struct{ Name string }) (*bool, error) {
	// implement proper logic
	status := true
	return &status, nil
}

// UpdateDataset implements updateDataset of graphql schema
func (r *Resolver) UpdateDataset(ctx context.Context, args struct{ Dataset datasetInput }) (*bool, error) {
	// implement proper logic
	status := true
	return &status, nil
}

// DeleteDataset implements deleteDataset of graphql schema
func (r *Resolver) DeleteDataset(ctx context.Context, args struct{ Name string }) (*bool, error) {
	// implement proper logic
	status := true
	return &status, nil
}

// datasetInput defines how client can post requests about dataset
type datasetInput struct {
	Name string
}

// encode cursor encodes the cursor position in base64
func encodeCursor(i int) graphql.ID {
	return graphql.ID(base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("cursor%d", i))))
}

// decode cursor decodes the base 64 encoded cursor and resturns the integer
func decodeCursor(s string) (int, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return 0, err
	}

	i, err := strconv.Atoi(strings.TrimPrefix(string(b), "cursor"))
	if err != nil {
		return 0, err
	}

	return i, nil
}
