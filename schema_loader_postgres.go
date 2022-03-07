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
)

const POSTGRES = `postgres`

func init() {

	RegisterLoader(POSTGRES, new(PostgresLoader))
}

type PostgresLoader struct {
}

func (self *PostgresLoader) getSchemnaDef(database string) string {

	ret := fmt.Sprintf(`select  
table_name,
udt_name as type_name,
column_name 
from INFORMATION_SCHEMA.COLUMNS 
where table_catalog = '%s'
and table_schema = 'public'`,
		database,
	)

	//	if len(tables) > 0 {
	//		_tables := []string{}
	//		for _, t := range tables {
	//			_tables = append(_tables, fmt.Sprintf(`'%s'`, t))
	//		}
	//		return fmt.Sprintf(`%s table_name in ( %s )`, strings.Join(_tables, " , "))
	//	}
	return ret
}

func (self *PostgresLoader) GetColumns(db *Msi) ([]map[string]interface{}, error) {
	query := self.getSchemnaDef(db.DatabaseName)
	typeMap := map[string]string{
		`table_name`:  `string`,
		`column_name`: `string`,

		`type_name`: `string`,
	}

	return db.Map(db.Db, query, typeMap)
}

func (self *PostgresLoader) DataType(dt string) string {
	kFieldTypes := map[string]string{
		"bigint":    "int64",
		"int":       "int",
		"integer":   "int",
		"smallint":  "int",
		"character": "string",
		"text":      "string",
		"timestamp": "time.Time",
		"numeric":   "float64",
		"boolean":   "bool", //timestamp without time zone
	}
	dt = strings.Split(dt, " ")[0]
	if fieldType, ok := kFieldTypes[strings.ToLower(dt)]; !ok {
		return "string"
	} else {
		return fieldType
	}
}

func (self *PostgresLoader) LoadDatabaseSchema(db *Msi) error {

	columns, err := self.GetColumns(db)
	if err != nil {
		return err
	}

	getTableColumns := func(tableName string, table *Table) ([]*Field, error) {
		ret := []*Field{}
		for _, col := range columns {
			_tableName, err := ToString(col[`table_name`])
			if err != nil {
				return nil, err
			}
			if _tableName != tableName {
				continue
			}
			_colName, err := ToString(col[`column_name`])
			if err != nil {
				return nil, err
			}
			_colType, err := ToString(col[`type_name`])
			if err != nil {
				return nil, err
			}

			ret = append(ret, &Field{
				table:    table,
				Name:     _colName,
				Type:     self.DataType(_colType),
				IsNumber: IsNumber(_colType),
				Selected: true,
			})
		}
		return ret, nil
	}
	//dsnString, schema, tableNames string
	//	db, err := sql.Open(MSSQL, db.DsnString)
	//TODO allow user cherry picking which tables
	getTableQuery := fmt.Sprintf(
		` 
		select  
table_name as name  
from INFORMATION_SCHEMA.TABLES  
where table_schema = 'public'
and table_catalog= '%s'
		
;`, db.DatabaseName,
	)

	typeMap := map[string]string{
		`name`: `string`,
	}

	tables, err := db.Map(db.Db, getTableQuery, typeMap)
	if err != nil {
		return err
	}

	for _, _table := range tables {
		tableName, err := ToString(_table[`name`])
		if err != nil {
			return err
		}

		table := new(Table)
		table.LifeCycle = new(LifeCycle)
		table.TableName = tableName

		table.Schema = db
		table.Limit = DEFAULT_LIMIT

		table.Fields, err = getTableColumns(table.TableName, table)
		if err != nil {
			return err
		}
		db.Tables = append(db.Tables, table)
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
