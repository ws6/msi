package msi

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/mijia/modelq/drivers" //thank you mijia
)

var (
	DEBUG = false
)

type M map[string]interface{}

type BeforeUpdate func(crit M, updates M) error
type AfterUpdate func(crit M, updates M) error

type BeforeRemove func(crit M) error
type AfterRemove func(crit M) error

type BeforeFind func(others ...map[string]interface{}) error

//function called on Find().Map()
type AfterFind func(results []map[string]interface{}) error // for the purpose of overwrite/block some fields

//e.g. userpassword->null
type BeforeCreate func(updates M) error
type AfterCreate func(updates M) error

type LifeCycle struct {
	BeforeUpdates []BeforeUpdate
	AfterUpdates  []AfterUpdate

	BeforeRemoves []BeforeRemove
	AfterRemoves  []AfterRemove

	BeforeFinds []BeforeFind
	AfterFinds  []AfterFind

	BeforeCreates []BeforeCreate
	AfterCreates  []AfterCreate
}

type Msi struct {
	Tables            []*Table
	*LifeCycle        //global lifecycle; note there is a table level lifecycle as well
	Db                *sql.DB
	DriverName        string
	DsnString         string //mysql or postgre
	DatabaseName      string //database name or schema in postgres
	ForeignKeyTypeMap map[string]string
}

func (self *Msi) GetTable(tableName string) *Table {
	for _, table := range self.Tables {
		if table.TableName == tableName {
			return table
		}
	}
	return nil
}

//dont forgot to close
func (self *Msi) Close() error {
	if self.Db != nil {
		return self.Db.Close() //provide a Close interface
	}

	return nil
}

//NewDb loading all tables field definitions from database
//NewDb(`mysql`, `rw_sage:Exxxc0ndid0@(ussd-prd-mysq01:3306)/sage`, `sage`,``)
func NewDb(driver, dsnString, schema, tableNames string) (*Msi, error) {
	ret := new(Msi)
	ret.DriverName = driver
	ret.LifeCycle = new(LifeCycle)
	ret.DatabaseName = schema
	dbSchema, err := drivers.LoadDatabaseSchema(driver, dsnString, schema, tableNames)

	if err != nil {
		return nil, err
	}

	ret.Db, err = sql.Open(driver, dsnString)

	if err != nil {
		return nil, err
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
			table.Fields = append(table.Fields, field)
		}
		ret.Tables = append(ret.Tables, table)

	}

	if ret.DriverName == `mysql` {
		if err := LoadMySqlForeignKeys(ret); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func mysqlForeignkeyQuery(dbname string) string {
	return fmt.Sprintf(`SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    REFERENCED_TABLE_NAME,
    REFERENCED_COLUMN_NAME
FROM
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE
    REFERENCED_TABLE_SCHEMA = '%s'`, dbname)
}

var foreignKeyMap = map[string]string{
	`TABLE_NAME`:             `string`,
	`COLUMN_NAME`:            `string`,
	`REFERENCED_TABLE_NAME`:  `string`,
	`REFERENCED_COLUMN_NAME`: `string`,
}

func getTableFromInterface(db *Msi, _table interface{}) (*Table, error) {
	//	_table, ok := i
	//	if !ok {
	//		return nil, fmt.Errorf(`can not find REFERENCED_TABLE_NAME`)
	//	}
	tn, ok := _table.(string)
	if !ok {
		return nil, fmt.Errorf(`can not type cast  REFERENCED_TABLE_NAME into string`)
	}

	ret := db.GetTable(tn)
	if ret == nil {
		return nil, fmt.Errorf(`not found table %s`, tn)
	}
	return ret, nil
}

func getFieldFromInterface(table *Table, _col interface{}) (*Field, error) {
	//	_col, ok := m[`REFERENCED_COLUMN_NAME`]

	//	if !ok {
	//		return nil, fmt.Errorf(`no REFERENCED_COLUMN_NAME found`)
	//	}

	col, ok := _col.(string)
	if !ok {
		return nil, fmt.Errorf(`col name is not string`)
	}
	field := table.GetField(col)
	if field == nil {
		return nil, fmt.Errorf(`field [%s] not found `, col)
	}

	return field, nil

}

func LoadMySqlForeignKeys(db *Msi) error {

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

		if DEBUG {
			log.Printf(`installed foreign key for %s.%s refers to->%s.%s `,
				table.TableName, currentCol.Name,
				refTable.TableName, foreignCol.Name,
			)
		}

	}
	return nil
}
