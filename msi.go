package msi

import (
	"fmt"
	"strings"

	"github.com/mijia/modelq/drivers" //thank you mijia
)

type Msi struct {
	Tables       []*Table
	DsnString    string //mysql or postgre
	DatabaseName string //database name or schema in postgres
}

func (self *Msi) GetTable(tableName string) *Table {

	for _, table := range self.Tables {
		if table.TableName == tableName {
			return table
		}
	}

	return nil
}

//NewMsi loading all tables field definitions from database
//NewMsi(`mysql`, `rw_sage:Exxxc0ndid0@(ussd-prd-mysq01:3306)/sage`, `sage`,``)

func NewMsi(driver, dsnString, schema, tableNames string) (*Msi, error) {
	ret := new(Msi)
	ret.DatabaseName = schema
	dbSchema, err := drivers.LoadDatabaseSchema(driver, dsnString, schema, tableNames)
	if err != nil {
		return nil, err
	}

	for tbl, cols := range dbSchema {

		table := new(Table)
		table.TableName = tbl
		for _, col := range cols {
			field := &Field{
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
	return ret, nil
}
