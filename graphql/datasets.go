package graphql

import (
	"context"
)

// User is the base user model to be used throughout the app
type Dataset struct {
	Name string
}

// DatasetResolver provides dataset resolver
type DatasetResolver struct {
	d Dataset
}

// Name resolves the Name field for Dataset
func (dr *DatasetResolver) Name(ctx context.Context) *string {
	return &dr.d.Name
}
