package graphql

import (
	"context"
)

// Dataset represents data model struct
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
