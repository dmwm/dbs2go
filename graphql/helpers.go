package graphql

import (
	"database/sql"
	"io/ioutil"

	graphql "github.com/graph-gophers/graphql-go"
)

// InitSchema initializes GraphQL schema
func InitSchema(fname string, db *sql.DB) *graphql.Schema {
	s, err := getSchema(fname)
	if err != nil {
		panic(err)
	}
	schema := graphql.MustParseSchema(s, &Resolver{db: db}, graphql.UseStringDescriptions())
	return schema
}

// helper function to get the graphql schema
func getSchema(path string) (string, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}

	return string(b), nil
}
