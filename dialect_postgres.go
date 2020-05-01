package msi

import (
	"fmt"
	"strings"
)

func init() {
	dialector[POSTGRES] = new(PostgresLoader) //syntax help. no usage
}

func (self *PostgresLoader) getTableName(t *Table) string {
	if t.Schema == nil {
		return t.TableName
	}

	return fmt.Sprintf(`public."%s" `,
		t.TableName,
	)
}

func (self *PostgresLoader) FullName(f *Field) string {
	if f.table == nil {
		return f.Name
	}
	//TODO check if it is postgres?
	return fmt.Sprintf(`public."%s"."%s"`, f.table.TableName, f.Name)
}

func (self *PostgresLoader) FullNameAS(f *Field, k, tableAlias string) string { // useing double underscores for uniqueness
	if f.table == nil {
		return f.Name
	}

	return fmt.Sprintf(`%s."%s" AS %s__%s`, tableAlias, f.Name, k, f.Name)
}

func (self *PostgresLoader) find(t *Table, others ...map[string]interface{}) (selectedFields []string, nonSelectClause string, orderby []string, limit int, offset int, err error) {
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
				fmt.Sprintf(` left join %s  %s on %s."%s" = %s `,
					self.getTableName(field.ReferencedTable),
					tableAlias,
					tableAlias,
					field.ReferencedField.Name,
					//					 t.TableName, fieldName,
					self.FullName(field),
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
				selectedFields = append(selectedFields, fmt.Sprintf("%s.*", t.TableName))
			}
			for _, k := range foreignFields {
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

func (self *PostgresLoader) FindQuery(t *Table, crit ...map[string]interface{}) (string, error) {

	//!!!below is mysql dialect by default
	selectedFields, nonSelectClause, orderby, limit, offset, err := self.find(t, crit...)
	if err != nil {
		return "", err
	}
	ret := fmt.Sprintf(`SELECT %s %s`, strings.Join(selectedFields, ", "), nonSelectClause)

	if len(orderby) > 0 {
		ret = fmt.Sprintf(`%s ORDER BY %s`, ret, strings.Join(orderby, " ,"))
	}

	if limit == 0 {
		limit = t.Limit //in case not init from NewDb
	}

	if limit > 0 {
		ret = fmt.Sprintf(`%s LIMIT %d`, ret, limit)
	}
	if offset > 0 {
		ret = fmt.Sprintf(`%s OFFSET %d`, ret, offset)
	}

	return ret, nil

}

func (self *PostgresLoader) CountQuery(t *Table, others ...map[string]interface{}) (string, error) {

	_, nonSelectClause, _, _, _, err := self.find(t, others...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`SELECT count(*) as count %s`, nonSelectClause), nil

}

func (self *PostgresLoader) InsertQuery(t *Table, _updates map[string]interface{}) (string, error) {

	updates := []*NameVal{}
	for k, v := range Stringify(POSTGRES, _updates) {

		if t.GetMyField(k) == nil {
			continue //remove non-table defined fields
		}

		updates = append(updates, &NameVal{k, v})
	}

	return fmt.Sprintf(
		`INSERT INTO %s ( %s ) VALUES ( %s) ;`,
		self.getTableName(t),
		strings.Join(t.MakeInsertFields(updates), ","),
		strings.Join(t.MakeInsertValues(updates), ","),
	), nil

}

func (self *PostgresLoader) GetForeignTableMap(t *Table) map[string]string {
	ret := make(map[string]string)
	for _, f := range t.Fields {
		if f.ReferencedField == nil || f.ReferencedTable == nil {
			continue
		}
		for _, rf := range f.ReferencedTable.Fields {
			postgresK := fmt.Sprintf(`%s."%s"`, f.GetTableAlias(), rf.Name)
			ret[postgresK] = rf.Type
			//!!!unsafe key supports the URL hack. if key contains keywords, caller has to quote it.
			unsafeKey := fmt.Sprintf("%s.%s", f.GetTableAlias(), rf.Name)
			ret[unsafeKey] = rf.Type
		}
	}
	return ret
}

func (self *PostgresLoader) SafeWhere(t *Table, crit map[string]interface{}) (string, error) {
	if crit == nil {
		return "", nil
	}
	//build select query

	_wheres, err := ParseCrit(crit)
	if err != nil {
		return "", err
	}

	wheres := []*Where{}
	tableTypeMap := self.GetForeignTableMap(t)
	allowedForeignKeyField := func(wf string) string {
		if newKey, ok := tableTypeMap[wf]; ok {
			return newKey
		}
		return ""
	}
	for _, where := range _wheres {

		//!!!exception for special format foreign keys; and dont re-write it
		// and Project__project_id."projectname" =  'Yale_Grigorenko_2'
		newKey := allowedForeignKeyField(where.FieldName)
		if newKey != "" {
			//			where.FieldName = newKey
			wheres = append(wheres, where)
			continue
		}

		safeFieldName := fmt.Sprintf(`public."%s"."%s"`, t.TableName, where.FieldName)
		for _, field := range t.Fields {
			//loosing the checker by allow tablename.fieldname format
			if field.Name == where.FieldName {
				where.FieldName = safeFieldName
				wheres = append(wheres, where)
				continue
			}

		}
	}

	whereClause := fmt.Sprintf(`WHERE 1=1  %s`, ToWhereString(t, wheres))

	return whereClause, nil

}

func (self *PostgresLoader) SafeUpdate(t *Table, updates map[string]interface{}) []string {
	up := []string{}
	for k, v := range Stringify(POSTGRES, updates) {
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
		up = append(up, fmt.Sprintf("%s.%s=%s", t.TableName, k, _v))
	}
	return up
}

func (self *PostgresLoader) UpdateQuery(t *Table, crit, updates map[string]interface{}) (string, error) {
	//	up := self.SafeUpdate(t, updates)
	up := t.SafeUpdate(updates)
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
	fmt.Println(ret)
	return ret, nil
}

//(t *Table) RemoveQuery(crit map[string]interface{}) (string, error)
func (self *PostgresLoader) RemoveQuery(t *Table, crit map[string]interface{}) (string, error) {
	whereClause, err := self.SafeWhere(t, crit)
	if err != nil {
		return "", err
	}
	ret := fmt.Sprintf(`DELETE FROM %s %s `, self.getTableName(t), whereClause)
	return ret, nil
}

func (self *PostgresLoader) GetGroupCountPage(t *Table, others ...map[string]interface{}) (*Page, error) {

	return nil, ERR_USE_MYSQL

}

func (self *PostgresLoader) GetSinceCountPage(t *Table, others ...map[string]interface{}) (*Page, error) {

	return nil, ERR_USE_MYSQL
}
