package msi

import (
	"fmt"
	"strings"
	"sync"
)

func init() {
	dialector[MSSQL] = new(MSSQLLoader) //syntax help. no usage
}

var ERR_NOT_IMPL = fmt.Errorf(`not implemented`)

func (self *MSSQLLoader) getTableName(t *Table) string {
	if t.Schema == nil {
		return t.TableName
	}

	return fmt.Sprintf("[%s].[dbo].[%s]",
		t.Schema.DatabaseName,
		t.TableName)
}

func (m *MSSQLLoader) FullName(self *Field) string {
	if self.table == nil {
		return self.Name
	}
	//TODO check if it is postgres?
	return fmt.Sprintf("[%s].[%s]", self.table.TableName, self.Name)
}

func (m *MSSQLLoader) FullNameAS(self *Field, k, tableAlias string) string { // useing double underscores for uniqueness
	if self.table == nil {
		return self.Name
	}

	return fmt.Sprintf("[%s].[%s] AS %s__%s", tableAlias, self.Name, k, self.Name)
}

func (self *MSSQLLoader) find(t *Table, others ...map[string]interface{}) (selectedFields []string, nonSelectClause string, orderby []string, limit int, offset int, err error) {
	for _, field := range t.Fields {
		if field.Selected {
			selectedFields = append(selectedFields, self.FullName(field))
		}
	}

	defer func() {
		if len(selectedFields) == 0 {
			selectedFields = []string{`*`} //not sufficient
		}
	}()

	nonSelectClause = fmt.Sprintf("FROM %s",
		self.getTableName(t),
	)

	if len(others) == 0 {
		return
	}

	var crit map[string]interface{}

	if len(others) > 0 {
		crit = others[0]
	}

	whereClause, _err := self.SafeWhere(t, crit)
	if _err != nil {
		err = _err
		return
	}
	if len(others) == 1 {
		nonSelectClause = fmt.Sprintf(`%s %s `, nonSelectClause, whereClause)
		return
	}

	//if len(others) > 1 {
	mq, _err := t.ParseMetaQuery(others[1])
	if err != nil {
		err = _err
		return
	}

	if len(mq.Fields) > 0 {
		selectedFields = mq.Fields //!!!overwrite
	}

	nonSelectClause = fmt.Sprintf("FROM %s  ", self.getTableName(t))

	//install joins; Please note joins are free form.
	if len(mq.Joins) > 0 {
		//TODO adding joins
		nonSelectClause = fmt.Sprintf("%s  %s", nonSelectClause, strings.Join(mq.Joins, " "))
	}

	if len(mq.Populates) > 0 {
		//install populates left joins
		leftjoins := []string{}
		for _, populate := range mq.Populates {
			_sp := strings.Split(populate, ":")
			if len(_sp) == 0 {
				continue
			}

			fieldName := _sp[0]

			field := t.GetField(fieldName)

			if field == nil {
				err = fmt.Errorf(`no field name [%s] found from table [%s]`, fieldName, t.TableName)
				return
			}
			if !field.Selected {
				continue // using for hidding certain fields. e.g. user->password field
			}

			//check foreignkey associations
			if field.ReferencedTable == nil {
				err = fmt.Errorf(`no foreign table   installed for col [%s] from table [%s]`, fieldName, t.TableName)
				return
			}

			if field.ReferencedField == nil {
				err = fmt.Errorf(`no foreign column   installed for col [%s] from table[%s]`, fieldName, t.TableName)
				return
			}
			//			tableAlias := fmt.Sprintf("%s__%s", field.ReferencedTable.TableName, field.Name)
			tableAlias := field.GetTableAlias()
			leftjoins = append(leftjoins,
				fmt.Sprintf(` left join %s  %s on %s.%s = %s.%s `,
					self.getTableName(field.ReferencedTable),
					tableAlias,
					tableAlias,
					field.ReferencedField.Name,
					t.TableName, fieldName,
				),
			)

			foreignFields := []string{}
			if len(_sp) > 1 {
				//rest are selected foreign cols
				_cols := strings.Split(_sp[1], ",")
				for _, _col := range _cols {

					foreignField := field.ReferencedTable.GetField(_col)
					if foreignField == nil {
						err = fmt.Errorf(`no foreign field found [%s]`, _col)
						return
					}
					foreignFields = append(foreignFields, self.FullNameAS(foreignField, fieldName, tableAlias))
				}
			}

			if len(foreignFields) == 0 {
				//use all foreign table fields
				for _, field := range field.ReferencedTable.Fields {

					if field.Hide == true {
						continue
					}

					foreignFields = append(foreignFields, self.FullNameAS(field, fieldName, tableAlias))
				}
			}
			if len(selectedFields) == 0 {
				selectedFields = append(selectedFields, fmt.Sprintf("[%s].*", t.TableName))
			}
			for _, k := range foreignFields {
				//TODO safe guard foreignFields
				selectedFields = append(selectedFields, k)
			}
		}

		nonSelectClause = fmt.Sprintf("%s  %s", nonSelectClause, strings.Join(leftjoins, " "))
	}

	//install nonSelectClause
	nonSelectClause = fmt.Sprintf("%s %s", nonSelectClause, whereClause)

	//intall groupby
	if len(mq.GroupBy) > 0 {
		//TODO to add field checker?
		nonSelectClause = fmt.Sprintf(`%s GROUP BY %s`, nonSelectClause, strings.Join(mq.GroupBy, " ,"))
	}
	//TODO install group countby then replace the select

	if len(mq.GroupCountBy) > 0 {
		if len(mq.GroupBy) > 0 {
			err = fmt.Errorf(`can not mix group and group count`)
			return
		}

		selectedFields = []string{} //clean out previous
		for _, f := range mq.GroupCountBy {
			selectedFields = append(selectedFields, f)
		}
		selectedFields = append(selectedFields, `count(*) as count`)
		nonSelectClause = fmt.Sprintf(`%s GROUP BY %s`, nonSelectClause, strings.Join(mq.GroupCountBy, " ,"))

	}

	if len(mq.Orderby) > 0 {
		orderby = mq.Orderby
	}

	limit = mq.Limit
	offset = mq.Offset

	if len(others) == 2 {
		return
	}
	//TODO adding joins, in a simple syntax

	joins := others[2]
	//TODO parse joins

	_ = joins
	return
}

func (self *MSSQLLoader) FindQuery(t *Table, crit ...map[string]interface{}) (string, error) {

	//!!!below is mysql dialect by default
	selectedFields, nonSelectClause, orderby, limit, offset, err := self.find(t, crit...)
	if err != nil {
		return "", err
	}
	//TODO safe guard selectedField with square brackets
	ret := fmt.Sprintf(`SELECT %s %s`, strings.Join(selectedFields, ", "), nonSelectClause)

	//install orderby
	if len(orderby) == 0 {
		//!!!use first field instead of saying "id"
		for _, f := range t.Fields {
			orderby = append(orderby, self.FullName(f))
			break
		}
	}

	if len(orderby) > 0 {
		ret = fmt.Sprintf(`%s ORDER BY %s`, ret, strings.Join(orderby, " ,"))
	}

	if limit == 0 {
		limit = t.Limit //in case not init from NewDb
	}

	//!!! for MSSQL, limit and offset are manditory
	if limit > 0 {
		ret = fmt.Sprintf(`%s OFFSET %d ROWS`, ret, offset)

		ret = fmt.Sprintf(`%s FETCH NEXT %d ROWS ONLY `, ret, limit)
	}

	return ret, nil

}

func (self *MSSQLLoader) CountQuery(t *Table, others ...map[string]interface{}) (string, error) {

	_, nonSelectClause, _, _, _, err := self.find(t, others...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`SELECT count(*) as count %s`, nonSelectClause), nil

}

func (self *MSSQLLoader) InsertQuery(t *Table, _updates map[string]interface{}) (string, error) {
	q, err := self.insertQuery(t, _updates)
	if err != nil {
		return "", err
	}

	return q, nil
}

func (self *MSSQLLoader) MakeInsertFields(t *Table, updates []*NameVal) []string {
	ret := []string{}

	for _, item := range updates {
		k := item.Name
		if _f := t.GetField(k); _f == nil {
			continue
		}

		ret = append(ret, fmt.Sprintf("[%s].[%s]", t.TableName, k))
	}

	return ret
}

func (self *MSSQLLoader) insertQuery(t *Table, _updates map[string]interface{}) (string, error) {

	updates := []*NameVal{}

	for k, v := range Stringify(_updates) {

		if t.GetMyField(k) == nil {
			continue //remove non-table defined fields
		}

		updates = append(updates, &NameVal{k, v})
	}

	return fmt.Sprintf(
		`INSERT INTO %s ( %s ) VALUES ( %s) ;`,
		self.getTableName(t),
		strings.Join(self.MakeInsertFields(t, updates), ","),
		strings.Join(t.MakeInsertValues(updates), ","),
	), nil

}

func (self *MSSQLLoader) SafeWhere(t *Table, crit map[string]interface{}) (string, error) {
	if crit == nil {
		return "", nil
	}
	//build select query

	_wheres, err := ParseCrit(crit)
	if err != nil {
		return "", err
	}

	wheres := []*Where{}
	tableTypeMap := t.GetForeignTableMap()
	allowedForeignKeyField := func(wf string) bool {
		if _, ok := tableTypeMap[wf]; ok {
			return true
		}
		return false
	}
	for _, where := range _wheres {
		//!!!exception for special format foreign keys; and dont re-write it
		if allowedForeignKeyField(where.FieldName) {

			wheres = append(wheres, where)
			continue
		}
		where.FieldName = fmt.Sprintf(`[%s].[%s]`, t.TableName, where.FieldName)
		for _, field := range t.Fields {
			//loosing the checker by allow tablename.fieldname format
			if field.Name == where.FieldName || fmt.Sprintf(`[%s].[%s]`, t.TableName, field.Name) == where.FieldName {
				wheres = append(wheres, where)
				continue
			}

		}
	}

	whereClause := fmt.Sprintf(`WHERE 1=1  %s`, ToWhereString(wheres))

	return whereClause, nil

}

func (self *MSSQLLoader) SafeUpdate(t *Table, updates map[string]interface{}) []string {
	up := []string{}
	for k, v := range Stringify(updates) {
		field := t.GetField(k)
		if field == nil {
			continue
		}
		if field.Name == `id` {
			continue //!!! biased design
		}

		_v := v

		if !field.IsNumber {
			_v = fmt.Sprintf("%s", _v)
		}
		found := t.GetMyField(k)
		if found == nil {
			continue //wash out the bad fields
		}
		up = append(up, fmt.Sprintf("[%s].[%s]=%s", t.TableName, k, _v))
	}
	return up
}

func (self *MSSQLLoader) UpdateQuery(t *Table, crit, updates map[string]interface{}) (string, error) {
	up := self.SafeUpdate(t, updates)

	ret := fmt.Sprintf(`UPDATE %s SET %s`,
		self.getTableName(t),
		strings.Join(up, ", "),
	)
	if crit == nil {
		return fmt.Sprintf("%s ;", ret), nil
	}
	whereClause, err := self.SafeWhere(t, crit)
	if err != nil {
		return "", err
	}

	ret = fmt.Sprintf(`%s %s;`, ret, whereClause)

	return ret, nil
}

//(t *Table) RemoveQuery(crit map[string]interface{}) (string, error)
func (self *MSSQLLoader) RemoveQuery(t *Table, crit map[string]interface{}) (string, error) {
	whereClause, err := self.SafeWhere(t, crit)
	if err != nil {
		return "", err
	}
	ret := fmt.Sprintf(`DELETE FROM %s %s `, self.getTableName(t), whereClause)
	return ret, nil
}

func (self *MSSQLLoader) GetGroupCountPage(t *Table, others ...map[string]interface{}) (*Page, error) {
	ret := new(Page)
	ret.Limit = t.Limit

	_, _, _, limit, offset, err := self.find(t, others...)
	if err != nil {
		return nil, err
	}

	if limit != 0 {
		ret.Limit = limit
	}
	var wg sync.WaitGroup

	wg.Add(2)

	go func(_wg *sync.WaitGroup) {
		ret.Offset = offset

		ret.Data, ret.FindErr = t.Find(others...).Map(map[string]string{`count`: `int`})
		_wg.Done()
	}(&wg)

	go func(_wg *sync.WaitGroup) {

		ret.Total, ret.CountErr = self.GetGroupCountPageCount(t, others...)
		_wg.Done()
	}(&wg)
	wg.Wait()

	if ret.FindErr != nil {
		return nil, fmt.Errorf(`find err: %s`, ret.FindErr.Error())
	}
	if ret.CountErr != nil {
		return nil, fmt.Errorf(`count err: %s`, ret.CountErr.Error())
	}
	return ret, nil
	return nil, ERR_USE_MYSQL

}

func (self *MSSQLLoader) GetGroupCountPageCount(t *Table, others ...map[string]interface{}) (int, error) {
	selectedFields, nonSelectClause, _, _, _, err := self.find(t, others...)
	if err != nil {
		return 0, err
	}
	rawQuery := fmt.Sprintf(`SELECT %s %s`, strings.Join(selectedFields, ", "), nonSelectClause)

	countQuery := fmt.Sprintf(`SELECT count(*) as count FROM (%s) temp`, rawQuery)
	if DEBUG {
		fmt.Println(`MSSQLLoader countQuery`, countQuery)
	}
	if DEBUG {
		fmt.Println(countQuery)
	}
	if t.Schema == nil {
		return 0, fmt.Errorf(`no schema pointer found from table[%s]`, t.TableName)
	}
	rows, err := t.Schema.Db.Query(countQuery)
	if DEBUG {
		fmt.Println(`MSSQLLoader rawQuery`, rawQuery)
	}
	if err != nil {

		return 0, fmt.Errorf(`countQuery err:%s`, err.Error())
	}
	defer rows.Close()

	var total int
	for rows.Next() {
		err := rows.Scan(&total)
		if err != nil {
			return 0, err
		}
		return total, nil
	}
	return 0, nil
}
