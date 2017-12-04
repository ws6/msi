package msi

import (
	"github.com/mijia/modelq/drivers"
)

//schema_loader
//depends on which driver used, it will do two things.
//1. load tables and table columns definitions.
//2. try to load foreign key associations

var Loaders = map[string]ShemaLoader{}

type DbSchema drivers.DbSchema

type ShemaLoader interface {
	LoadForeignKeys(*Msi) error
	LoadDatabaseSchema(*Msi) (DbSchema, error)
}
