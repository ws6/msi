package msi

import (
	"github.com/mijia/modelq/drivers"
)

const MYSQL = `mysql`

func init() {
	Loaders[MYSQL] = new(MySqlLoader)
}

type MySqlLoader struct {
}

func (self *MySqlLoader) LoadDatabaseSchema(db *Msi) (DbSchema, error) {

	ret, err := drivers.LoadDatabaseSchema(MYSQL, db.DsnString, db.DatabaseName, db.tableNames)
	if err != nil {
		return nil, err
	}
	return DbSchema(ret), nil
}

func (self *MySqlLoader) LoadForeignKeys(db *Msi) error {
	res, err := db.Map(db.Db, mysqlForeignkeyQuery(db.DatabaseName), foreignKeyMap)
	if err != nil {
		return err
	}

	for _, m := range res {
		refTable, err := getTableFromInterface(db, m[`REFERENCED_TABLE_NAME`])
		if err != nil {
			return err
		}
		table, err := getTableFromInterface(db, m[`TABLE_NAME`])
		if err != nil {
			return err
		}

		foreignCol, err := getFieldFromInterface(refTable, m[`REFERENCED_COLUMN_NAME`])
		if err != nil {
			return err
		}

		currentCol, err := getFieldFromInterface(table, m[`COLUMN_NAME`])
		if err != nil {
			return err
		}
		currentCol.ReferencedTable = refTable
		currentCol.ReferencedField = foreignCol

	}
	return nil

}
