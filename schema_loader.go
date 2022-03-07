package msi

import (
	"sync"

	"github.com/mijia/modelq/drivers"
)

//schema_loader
//depends on which driver used, it will do two things.
//1. load tables and table columns definitions.
//2. try to load foreign key associations

var RegisterLoader, GetLoader = func() (
	func(string, ShemaLoader),
	func(string) ShemaLoader,
) {
	loaders := make(map[string]ShemaLoader)
	var lock = sync.Mutex{}
	return func(name string, i ShemaLoader) {
			lock.Lock()
			loaders[name] = i
			lock.Unlock()
		}, func(name string) ShemaLoader {
			lock.Lock()
			found, ok := loaders[name]
			lock.Unlock()
			if ok {
				return found
			}
			return nil
		}
}()

type DbSchema drivers.DbSchema

type ShemaLoader interface {
	LoadForeignKeys(*Msi) error
	LoadDatabaseSchema(*Msi) error
}
