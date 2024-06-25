package msi

//dialect_mssql_populates2.go funcs for populates2 feature
import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

//CompilePopulates2 compile sql query for joins
//params:
//	t - the table its points to
//  key - the fk exists in table t
//	order- the depth of current join
//	allowedFields - which fields to expose; empty for all
//returns:
//
func (self *MSSQLLoader) CompilePopulates2(t *Table, keyOrAlt string, order int,
	preTableName, prePKName string,
	allowedFields []string) (
	nextTable *Table, //the fk table key is using
	newTempTableName string,
	newPKName string,
	selectedFields []string,
	nonSelectClause string,
	typeMap map[string]string,
	err error,
) {
	typeMap = make(map[string]string)
	sp := strings.Split(keyOrAlt, "@")
	key := sp[0]
	newTempTableName = fmt.Sprintf(`t%d__%s`, order, key) //unique

	if len(sp) > 1 {
		//user passed in alt name
		alt := sp[1] //be careful not using malformatted strings. before we invest more
		newTempTableName = alt
	}

	field := t.GetField(key)
	if field == nil {
		err = fmt.Errorf(`no ref table[%s]`, key)
		return
	}
	nextTable = field.ReferencedTable
	if nextTable == nil {
		err = fmt.Errorf(`nextTable is empty`)
		return
	}

	pk := t.GetFirstPrimaryKey()
	if pk == nil {
		err = fmt.Errorf(`no pk`)
		return
	}
	nextPK := nextTable.GetFirstPrimaryKey()

	if nextPK == nil {
		err = fmt.Errorf(`no nextPK`)
		log.Warn(err.Error())
		return
	}

	newPKName = fmt.Sprintf(`%s__%s`, newTempTableName, nextPK.Name)

	newFields := []string{}
	localAllowedField := []string{}
	for _, s := range allowedFields {
		if strings.HasPrefix(s, newTempTableName) {
			localAllowedField = append(localAllowedField, s)
		}
	}
	selectedFields = append(selectedFields, newPKName)
	typeMap[newPKName] = nextPK.Type

	for _, f := range nextTable.Fields {
		if f.Hide {
			continue
		}
		nk := fmt.Sprintf(`%s__%s`, newTempTableName, f.Name)
		if len(localAllowedField) > 0 && !StringInSlice(nk, localAllowedField) {
			continue
		}
		if nk == newPKName {
			continue
		}
		fieldFullName := fmt.Sprintf(` ,[%s].[%s] as [%s] `, nextTable.TableName, f.Name, nk)
		//TODO inject typemap
		newFields = append(newFields, fieldFullName)
		selectedFields = append(selectedFields, nk)
		typeMap[nk] = f.Type

	}
	fieldsStr := strings.Join(newFields, "\n")
	if preTableName == "" {
		preTableName = t.TableName
	}
	if prePKName == "" {
		prePKName = pk.Name
	}

	//add ref fields
	nonSelectClause = fmt.Sprintf(`
		LEFT JOIN (
 			 select 
			 [%s].[%s] --join key
 			, [%s].[%s]  as [%s]    -- new PK must exposed 
 			 --fields from table 
			%s
			-- end of fields from table 
 			from [%s]    
 			LEFT JOIN [%s]   on  [%s] .[%s] =   [%s].[%s]
 		) [%s] 
		on [%s].[%s] = [%s].[%s]
	`,
		t.TableName, pk.Name, //--join key
		//preTableName, prePKName,
		nextTable.TableName, nextPK.Name, newPKName, // -- new PK must exposed

		fieldsStr, //fields from table

		nextTable.TableName, //from

		t.TableName, t.TableName, key, nextTable.TableName, nextPK.Name, // left join
		newTempTableName,                                   //newTempTableName
		newTempTableName, pk.Name, preTableName, prePKName, // on

	)

	return
}

//CompilePopulates3 parsing user specified join statement. danger.
//CompilePopulates3 can join models not neccessarily a parent FK
func (self *MSSQLLoader) CompilePopulates3(t *Table,
	alias string,
	allowedFields []string,
	joinLeftPart, jointRightPart string) (

	selectedFields []string,
	nonSelectClause string,
	typeMap map[string]string,
	err error,

) {
	typeMap = make(map[string]string)
	leftSp := strings.Split(joinLeftPart, ".")
	if len(leftSp) < 2 {
		err = fmt.Errorf(`left part incorrect expect model.key`)
		return
	}
	leftTableName := leftSp[0]
	leftKey := leftSp[1]                          //we can support multiple keys join here
	rightSp := strings.Split(jointRightPart, ".") //right part must already exist in entire statement. othewise it could fail
	if len(rightSp) < 2 {
		err = fmt.Errorf(`right part incorrect expect model.key`)
		return
	}
	rightTableName := rightSp[0] //we dont need verify the rightTableName. it could be any table alias.
	rightKey := rightSp[1]

	//compile fields

	leftTable := t.GetMsi().GetTable(leftTableName)
	if leftTable == nil {
		err = fmt.Errorf(`no left table - %s`, leftTableName)
		return
	}
	localAllowedField := []string{}
	for _, s := range allowedFields {
		if strings.HasPrefix(s, alias) {
			localAllowedField = append(localAllowedField, s)
		}
	}
	newFields := []string{}
	for _, f := range leftTable.Fields {
		//check each field

		if f.Hide {
			continue
		}

		if f.Name == leftKey {
			continue
		}

		nk := fmt.Sprintf(`%s__%s`, alias, f.Name)
		if len(localAllowedField) > 0 && !StringInSlice(nk, localAllowedField) {
			continue
		}

		fieldFullName := fmt.Sprintf(` ,[%s].[%s] as [%s] `, leftTableName, f.Name, nk)
		//TODO inject typemap
		newFields = append(newFields, fieldFullName)
		selectedFields = append(selectedFields, nk)
		typeMap[nk] = f.Type

	}

	fieldsStr := strings.Join(newFields, "\n")
	leftKeyAlias := fmt.Sprintf(`%s__%s`, alias, leftKey)
	nonSelectClause = fmt.Sprintf(`
		LEFT JOIN (
 			 select 
			 %s as [%s]
			 %s
 			from [%s]    
 			 
 		) [%s] 
		on [%s].[%s] = [%s].[%s]
	`,
		leftKey, leftKeyAlias, //has to present
		fieldsStr,
		leftTableName,
		alias,
		alias, leftKeyAlias,
		rightTableName, rightKey,
	)

	return
}

//CompileAllPopulates2 convert from query formatter to query
func (self *MSSQLLoader) CompileAllPopulates2(t *Table, populates2 []string) (
	selectedFields []string,
	nonSelectClause []string,
	typeMap map[string]string,
	err error,
) {
	if len(populates2) == 0 {
		return
	}
	typeMap = make(map[string]string)
	for i, p2 := range populates2 {
		//process each item
		//full formatter  $keyX1-->$keyX2-->$keyX3:$field1,$filed2|$keyY1-->$keyY2-->$keyY3:$field1,$filed2
		//$field list is optional. if  not provided, it will use all.
		//one item =  $keyX1-->$keyX2-->$keyX3:$field1,$filed2
		sp1 := strings.Split(p2, ":") //first split the keys and fields
		if len(sp1) == 0 {
			err = fmt.Errorf(`p2 key is empty at pos[%d]`, i)
			return
		}
		if len(sp1) > 2 {
			continue //leaving to the end
		}
		comboKeys := strings.TrimSpace(sp1[0]) //comboKeys=$keyX1-->$keyX2-->$keyX3
		if comboKeys == "" {
			err = fmt.Errorf(`p2 key is empty or with spaces at pos[%d]`, i)
			return
		}
		fields := []string{}
		if len(sp1) == 2 {
			fields = strings.Split(sp1[1], ",")
		}

		//TODO use fields to control which select

		keys := strings.Split(comboKeys, "->") //comboKeys separator
		if len(keys) == 0 {                    //TODO adding where
			err = fmt.Errorf(`no keys found`)
			return
		}

		//TODO build joins and fields, update table's typemap
		currentTable := t
		preTableName := ""
		prePKName := ""
		// t *Table, key string, order int,
		// preTableName, prePKName string,
		// allowedFields []string

		for order, key := range keys {

			// nextTable *Table, //the fk table key is using
			// newTempTableName string,
			// newPKName string,
			// selectedFields []string,
			// nonSelectClause string,
			// err error,
			nextTable, newTempTableName, newPKName, _selectedFields, _nonSelectClause, _typeMap, _err := self.CompilePopulates2(currentTable, key, order, preTableName, prePKName, fields)
			if err != nil {
				err = _err
				return
			}
			currentTable = nextTable
			preTableName = newTempTableName
			prePKName = newPKName

			nonSelectClause = append(nonSelectClause, _nonSelectClause)
			selectedFields = append(selectedFields, _selectedFields...)

			for k, v := range _typeMap {
				t.SetExtraTypeMap(k, v)
				typeMap[k] = v
			}
		}

	}

	//insert v3 style pop at the end in case losing reference table.
	for i, p2 := range populates2 {
		sp1 := strings.Split(p2, ":") //first split the keys and fields
		if len(sp1) != 3 {
			continue //only deal with alias:left_table.left_key.right_table.right_key:fields
		}
		//e.g. ?_populates2=tmp:fastq_lane_dataset.library_id->asa_library.library_id:tmp_yield,tmp_status,tmp_id
		alias := sp1[0] //user responseble for non-duplciate alias
		joinStr := sp1[1]
		jsp := strings.Split(joinStr, "->")
		if len(jsp) != 2 {
			err = fmt.Errorf(`wrong join, expect fastq_lane_dataset.library_id->asa_library.library_id at[%d]`, i)
			return
		}

		fieldStr := sp1[2]
		fields := strings.Split(fieldStr, ",") //user reponsible for existing keys

		_selectedFields, _nonSelectClause, _typeMap, _err := self.CompilePopulates3(t, alias, fields, jsp[0], jsp[1])
		if _err != nil {
			err = _err
			return
		}

		nonSelectClause = append(nonSelectClause, _nonSelectClause)
		selectedFields = append(selectedFields, _selectedFields...)

		for k, v := range _typeMap {
			t.SetExtraTypeMap(k, v)
			typeMap[k] = v
		}

	}
	return
}
