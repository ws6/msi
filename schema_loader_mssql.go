package msi

import (
	"fmt"
	"strings"

	"github.com/mijia/modelq/drivers"
)

const MSSQL = `mssql`

func init() {
	Loaders[MSSQL] = new(MSSQLLoader)
}

type TableSchema drivers.TableSchema
type MSSQLLoader struct {
	Version string //version alter the limit/offset syntax
}

//!!!sql get tables
/*
use FtsReportingStage ;
SELECT   *

FROM FtsReportingStage.sys.tables

WHERE schema_id = SCHEMA_ID('dbo')
;

 SELECT   *

 FROM FtsReportingStage.sys.tables

 WHERE schema_id = SCHEMA_ID('dbo') and name in ('sample')
*/

//!!!get object_id
/*
use FtsReportingStage ;
SELECT   *

FROM FtsReportingStage.sys.objects
where type = 'U'
*/

//!!!get columns
/*
use FtsReportingStage ;
SELECT

 o.name as table_name,
c.name as column_name,
c.max_length as max_length ,
t.name as type_name
FROM FtsReportingStage.sys.columns  c
  join   FtsReportingStage.sys.objects o on o.object_id = c.object_id
  join    FtsReportingStage.sys.types t on t.system_type_id = c.system_type_id
 where o.type = 'U'
*/

//!!!get foreign key query
/*
select
po.name  as table_name ,
c.name as column_name ,

o.name as referece_table_name,
rc.name as reference_column_name ,
 self.name as fk_name
 from
 FtsReportingStage.sys.foreign_key_columns  f

  join  FtsReportingStage.sys.objects o on o.object_id = f.referenced_object_id
  join  FtsReportingStage.sys.objects po on po.object_id = f.parent_object_id
  join  FtsReportingStage.sys.columns c on c.column_id = f.parent_column_id  and c.object_id = f.parent_object_id
  join  FtsReportingStage.sys.columns rc on rc.column_id = f.referenced_column_id  and rc.object_id = f.referenced_object_id
  join  FtsReportingStage.sys.objects as self on self.object_id = f.constraint_object_id
*/

func (self *MSSQLLoader) DataType(colDataType string) string {

	kFieldTypes := map[string]string{
		"text":      "int64",
		"date":      "time.Time",
		"datetime":  "time.Time",
		"datetime2": "time.Time",
		"time":      "time.Time",

		"int":     "int",
		"tinyint": "int",
		"bigint":  "int64",

		"smallint": "int",

		"char":    "string",
		"varchar": "string",
		"nchar":   "string",
		"blob":    "[]byte",
		"binary":  "[]byte",

		"float":   "float64",
		"decimal": "float64",
		"double":  "float64",
		"bit":     "uint64",
		//		"numeric": "float64",
	}
	fieldType, ok := kFieldTypes[strings.ToLower(colDataType)]
	if !ok {
		//		for k, v := range kFieldTypes {
		//			if strings.HasPrefix(colDataType, k) {
		//				return v
		//			}
		//		}
		return "string"
	}
	return fieldType

}
func (self *MSSQLLoader) GetColumns(db *Msi) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`SELECT

 o.name as table_name,
c.name as column_name,
c.max_length as max_length ,
t.name as type_name
FROM %s.sys.columns  c
  join   %s.sys.objects o on o.object_id = c.object_id
  join    %s.sys.types t on t.system_type_id = c.system_type_id
 where o.type = 'U'`,
		db.DatabaseName,
		db.DatabaseName,
		db.DatabaseName,
	)
	typeMap := map[string]string{
		`table_name`:  `string`,
		`column_name`: `string`,
		`max_length`:  `int`,
		`type_name`:   `string`,
	}

	return db.Map(db.Db, query, typeMap)
}

func (self *MSSQLLoader) SetVersion(db *Msi) error {

	versionQuery := `SELECT @@VERSION as version`
	typeMap := map[string]string{
		`version`: `string`,
	}
	_versionInfo, err := db.Map(db.Db, versionQuery, typeMap)
	if err != nil {
		return err
	}

	if len(_versionInfo) == 0 {
		return fmt.Errorf(`no result found`)
	}
	versionInfo := _versionInfo[0]
	self.Version, err = ToString(versionInfo[`version`])

	return err

}

func (self *MSSQLLoader) LoadDatabaseSchema(db *Msi) error {
	if err := self.SetVersion(db); err != nil {
		return err
	}

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
		`SELECT   name

FROM %s.sys.tables

WHERE schema_id = SCHEMA_ID('dbo')
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

func (self *MSSQLLoader) LoadForeignKeys(db *Msi) error {
	query := fmt.Sprintf(`
	select
po.name  as table_name ,
c.name as column_name ,

o.name as referece_table_name,
rc.name as reference_column_name ,
 self.name as fk_name
 from
 %s.sys.foreign_key_columns  f

  join  %s.sys.objects o on o.object_id = f.referenced_object_id
  join  %s.sys.objects po on po.object_id = f.parent_object_id
  join  %s.sys.columns c on c.column_id = f.parent_column_id  and c.object_id = f.parent_object_id
  join  %s.sys.columns rc on rc.column_id = f.referenced_column_id  and rc.object_id = f.referenced_object_id
  join  %s.sys.objects as self on self.object_id = f.constraint_object_id`,
		db.DatabaseName,
		db.DatabaseName,
		db.DatabaseName,
		db.DatabaseName,
		db.DatabaseName,
		db.DatabaseName,
	)

	typeMap := map[string]string{
		`table_name`:            `string`,
		`column_name`:           `string`,
		`referece_table_name`:   `string`,
		`reference_column_name`: `string`,
		`fk_name`:               `string`,
	}

	foreignKeys, err := db.Map(db.Db, query, typeMap)
	if err != nil {
		return err
	}

	//TODO install each
	for _, fk := range foreignKeys {
		tableName, err := ToString(fk[`table_name`])
		if err != nil {
			return err
		}
		colName, err := ToString(fk[`column_name`])
		if err != nil {
			return err
		}
		refTableName, err := ToString(fk[`referece_table_name`])
		if err != nil {
			return err
		}
		refColName, err := ToString(fk[`reference_column_name`])
		if err != nil {
			return err
		}

		table := db.GetTable(tableName)
		if table == nil {
			return fmt.Errorf(`no table found [%s]`, tableName)
		}
		col := table.GetField(colName)
		if col == nil {
			return fmt.Errorf(`no col found for install foreign key: %s`, colName)
		}
		refTable := db.GetTable(refTableName)
		if table == nil {
			return fmt.Errorf(`no reference table found [%s]`, refTableName)
		}

		refCol := table.GetField(refColName)
		if col == nil {
			return fmt.Errorf(`no refernece  col found for install foreign key: %s`, refColName)
		}

		col.ReferencedTable = refTable
		col.ReferencedField = refCol
	}
	return nil
}
