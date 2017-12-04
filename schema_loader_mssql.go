package msi

import (
	"fmt"

	"github.com/mijia/modelq/drivers"
)

const MSSQL = `mssql`

func init() {
	//	Loaders[MSSQL] = new(MSSQLLoader)
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

func (self *MSSQLLoader) LoadDatabaseSchema(db *Msi) (DbSchema, error) {
	if err := self.SetVersion(db); err != nil {
		return nil, err
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
		return nil, err
	}
	ret := make(drivers.DbSchema)
	for _, _table := range tables {
		tableName, err := ToString(_table[`name`])
		if err != nil {
			return nil, err
		}

		_ = tableName
		if _, ok := ret[tableName]; !ok {
			ret[tableName] = make(drivers.TableSchema, 0, 5)
		}
		//TODO load columns

	}

	return nil, fmt.Errorf(`not implemented`)

}

func (self *MSSQLLoader) LoadForeignKeys(db *Msi) error {
	return fmt.Errorf(`not implemented`)
}
