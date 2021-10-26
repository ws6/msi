package msi

import (
	"fmt"

	"strings"
)

const SNOWFLAKE = `snowflake`

func init() {
	Loaders[SNOWFLAKE] = new(SnowflakeLoader)
}

type SnowflakeLoader struct{}

func (self *SnowflakeLoader) UpdateSchema(db *Msi) error {
	sp := strings.Split(db.DatabaseName, ".")
	if len(sp) == 0 {
		return fmt.Errorf(`expect to use databasename.schema`)
	}
	db.DatabaseName = strings.ToUpper(sp[0])
	db.Schema = `PUBLIC` //default
	if len(sp) >= 2 {
		db.Schema = strings.ToUpper(sp[1])
	}

	return nil
}
func (self *SnowflakeLoader) LoadForeignKeys(db *Msi) error {
	query := `SHOW IMPORTED KEYS`

	founds, err := db.Map(db.Db, query, nil)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}

	for _, found := range founds {

		pk_database_name, _ := ToString(found[`pk_database_name`])

		if pk_database_name != db.DatabaseName {
			continue
		}

		pk_schema_name, _ := ToString(found[`pk_schema_name`])
		if pk_schema_name != db.Schema {
			continue
		}
		pk_table_name, _ := ToString(found[`pk_table_name`])
		pk_column_name, _ := ToString(found[`pk_column_name`])

		table := db.GetTable(pk_table_name)
		if table == nil {
			continue
		}
		field := table.GetField(pk_column_name)
		if field == nil {
			continue
		}
		//TODO support multiple schema
		fk_database_name, _ := ToString(found[`fk_database_name`])
		fk_schema_name, _ := ToString(found[`fk_schema_name`])
		_ = fk_database_name
		_ = fk_schema_name

		fk_table_name, _ := ToString(found[`fk_table_name`])
		field.ReferencedTable = db.GetTable(fk_table_name)
		if field.ReferencedTable == nil {
			continue
		}
		fk_column_name, _ := ToString(found[`fk_column_name`])
		field.ReferencedField = field.ReferencedTable.GetField(fk_column_name)

	}
	return nil
}
func (self *SnowflakeLoader) LoadDatabaseSchema(db *Msi) error {
	//processing split databasename.schema for snowflake only
	if err := self.UpdateSchema(db); err != nil {
		return fmt.Errorf(`UpdateSchema:%s`, err.Error())
	}
	query := fmt.Sprintf(` 
	  select 
 *
 from "%s"."INFORMATION_SCHEMA"."TABLES" 
 where 1=1
  
  and table_catalog = '%s'
  and table_schema='%s'
	
	`,
		db.DatabaseName,

		db.DatabaseName,
		db.Schema,
	)
	typeMap := map[string]string{
		`table_name`:  `string`,
		`column_name`: `string`,
		`max_length`:  `int`,
		`type_name`:   `string`,
	}

	_ = typeMap

	founds, err := db.Map(db.Db, query, nil)
	if err != nil {
		return err
	}
	isSelectedTableName := func(s string) bool {

		if db.tableNames == "" {
			return true
		}
		sp := strings.Split(db.tableNames, ",")
		if len(sp) == 0 {
			return true
		}
		for _, tn := range sp {
			if strings.ToUpper(tn) == strings.ToUpper(s) {
				return true
			}
		}
		return false
	}
	for _, found := range founds {

		tableName, err := ToString(found[`TABLE_NAME`])
		if err != nil {
			return err
		}

		if !isSelectedTableName(tableName) {
			continue
		}

		topushTable := new(Table)
		topushTable.Schema = db
		topushTable.TableName = tableName
		if err := self.LoadTableFields(db, topushTable); err != nil {
			return err
		}
		db.Tables = append(db.Tables, topushTable)
		//add fields

	}

	return nil
}

func (self *SnowflakeLoader) LoadTableFields(db *Msi, table *Table) error {
	query := fmt.Sprintf(
		`select 
*
from 
"%s"."INFORMATION_SCHEMA"."COLUMNS"  
where 1=1
and table_schema = '%s'
and table_name = '%s'`,
		db.DatabaseName,
		db.Schema,
		table.TableName,
	)
	founds, err := db.Map(db.Db, query, nil)
	if err != nil {
		return err
	}
	primaryKeyInfo, err := self.GetPrimaryKey(db, table)
	if err != nil {
		return err
	}

	for _, found := range founds {
		topushField := new(Field)

		topushField.Name, err = ToString(found[`COLUMN_NAME`])
		if err != nil {
			return err
		}

		topushField.table = table //back reference
		table.Fields = append(table.Fields, topushField)
		topushField.Type, err = ToString(found[`DATA_TYPE`])
		if err != nil {
			return err
		}
		topushField.Length, _ = ToInt(found[`CHARACTER_MAXIMUM_LENGTH`])
		numericPrecision, _ := ToInt(found[`NUMERIC_PRECISION`])
		topushField.IsNumber = numericPrecision > 0

		if topushField.IsNumber {
			topushField.Length = numericPrecision
		}

		isNullable, _ := ToString(found[`IS_NULLABLE`])
		topushField.IsNullable = isNullable != `NO`

		if v, ok := primaryKeyInfo[topushField.Name]; ok && v {

			topushField.IsPrimaryKey = true
		}
	}

	return nil
}

//snowflake types
//https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html

func (self *SnowflakeLoader) GetPrimaryKey(db *Msi, table *Table) (map[string]bool, error) {
	query := fmt.Sprintf(
		` describe table   "%s"."%s"."%s" `,
		db.DatabaseName,
		db.Schema,
		table.TableName,
	)
	founds, err := db.Map(db.Db, query, nil)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]bool)

	for _, found := range founds {

		fieldName, err := ToString(found[`name`])
		if err != nil {
			return nil, err
		}
		ret[fieldName] = false

		if v, ok := found[`primary key`]; ok && v == `Y` {
			ret[fieldName] = true
		}

	}

	return ret, nil
}

//parse VARIANT type into json. only one level down

func (table *Table) MarshalVariantIntoJson(results []map[string]interface{}) error {
	// table.AfterFinds = append(table.AfterFinds, table.MarshalVariantIntoJson)
	for _, m := range results {
		for k, v := range m {
			field := table.GetField(k)
			if field == nil {
				continue
			}
			if field.Type != `VARIANT` {
				continue
			}
			//now we need do it
			//v is type of string. marshall it into msi.M
			_ = v
		}

	}
	return nil
}
