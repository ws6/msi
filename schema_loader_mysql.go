package msi

import (
	"fmt"
	"strings"

	"github.com/mijia/modelq/drivers"
)

const MYSQL = `mysql`

func init() {
	RegisterLoader(MYSQL, new(MySqlLoader))
}

type MySqlLoader struct {
}

func (self *MySqlLoader) LoadDatabaseSchema(ret *Msi) error {

	dbSchema, err := drivers.LoadDatabaseSchema(MYSQL, ret.DsnString, ret.DatabaseName, ret.tableNames)
	if err != nil {
		return err
	}

	for tbl, cols := range dbSchema {

		table := new(Table)
		table.LifeCycle = new(LifeCycle)
		table.TableName = tbl

		table.Schema = ret
		table.Limit = DEFAULT_LIMIT
		for _, col := range cols {
			field := &Field{
				table:    table,
				Name:     col.ColumnName,
				Type:     col.DataType,
				IsNumber: IsNumber(col.DataType),
				//TODO ParseLength
				IsNullable:      strings.ToUpper(col.IsNullable) == "YES",
				JsonMeta:        fmt.Sprintf("`json:\"%s\"`", col.ColumnName),
				IsPrimaryKey:    strings.ToUpper(col.ColumnKey) == "PRI",
				IsUniqueKey:     strings.ToUpper(col.ColumnKey) == "UNI",
				IsIndexed:       strings.ToUpper(col.ColumnKey) == "MUL",
				IsAutoIncrement: strings.ToUpper(col.Extra) == "AUTO_INCREMENT",
				DefaultValue:    col.DefaultValue,
				Extra:           col.Extra,
				Comment:         col.Comment,
			}
			field.Selected = true //!!! be aware default is selected unless unselected after loading

			table.Fields = append(table.Fields, field)
		}
		ret.Tables = append(ret.Tables, table)

	}
	return nil
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
