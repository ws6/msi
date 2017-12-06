package msi

//!!!NB: if user account is read_only will fail.
//https://stackoverflow.com/questions/1152260/postgres-sql-to-list-table-foreign-keys
//SELECT
//    tc.constraint_name, tc.table_name, kcu.column_name,
//    ccu.table_name AS foreign_table_name,
//    ccu.column_name AS foreign_column_name
//FROM
//    information_schema.table_constraints AS tc
//    JOIN information_schema.key_column_usage AS kcu
//      ON tc.constraint_name = kcu.constraint_name
//    JOIN information_schema.constraint_column_usage AS ccu
//      ON ccu.constraint_name = tc.constraint_name
//WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='mytable';

import (
	"fmt"
	"strings"

	"github.com/mijia/modelq/drivers"
)

const POSTGRES = `postgres`

func init() {
	Loaders[POSTGRES] = new(PostgresLoader)
}

type PostgresLoader struct {
}

func (self *PostgresLoader) LoadDatabaseSchema(ret *Msi) error {

	dbSchema, err := drivers.LoadDatabaseSchema(POSTGRES, ret.DsnString, ret.DatabaseName, ret.tableNames)
	if err != nil {
		return fmt.Errorf(`LoadDatabaseSchema err: %s`, err.Error())
	}

	for tbl, cols := range dbSchema {
		fmt.Println(`postgres table`, tbl)
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

var postgresFkTypeMape = map[string]string{
	`table_name`:          `string`,
	`column_name`:         `string`,
	`foreign_table_name`:  `string`,
	`foreign_column_name`: `string`,
}

func postgresForeignkeyQuery(dbname string) string {

	return fmt.Sprintf(`SELECT
     
    tc.table_name    , 
    kcu.column_name,
   ccu.table_name AS foreign_table_name,
   ccu.column_name AS foreign_column_name
FROM
    information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
     WHERE constraint_type = 'FOREIGN KEY'  `)
}

func (self *PostgresLoader) LoadForeignKeys(db *Msi) error {
	res, err := db.Map(db.Db, postgresForeignkeyQuery(db.DatabaseName), postgresFkTypeMape)
	if err != nil {
		return err
	}

	for _, m := range res {
		refTable, err := getTableFromInterface(db, m[`foreign_table_name`])
		if err != nil {
			return err
		}
		table, err := getTableFromInterface(db, m[`table_name`])
		if err != nil {
			return err
		}

		foreignCol, err := getFieldFromInterface(refTable, m[`foreign_column_name`])
		if err != nil {
			return err
		}

		currentCol, err := getFieldFromInterface(table, m[`column_name`])
		if err != nil {
			return err
		}
		currentCol.ReferencedTable = refTable
		currentCol.ReferencedField = foreignCol

	}
	return nil

}
