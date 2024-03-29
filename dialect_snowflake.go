package msi

import (
	"fmt"
	"log"

	"strings"
	"sync"
	"time"
)

func init() {
	dialector[SNOWFLAKE] = new(SnowflakeLoader)
}

func (self *SnowflakeLoader) getTableName(t *Table) string {
	if t.Schema == nil {
		return t.TableName
	}

	return fmt.Sprintf(`"%s"."%s"."%s"`,
		strings.ToUpper(t.Schema.DatabaseName),
		strings.ToUpper(t.Schema.Schema),
		strings.ToUpper(t.TableName),
	)
}

func (m *SnowflakeLoader) FullName(self *Field) string {
	if self.table == nil {
		return self.Name
	}
	//TODO check if it is postgres?
	return fmt.Sprintf(`%s."%s"`, m.getTableName(self.table), self.Name)
}

func (m *SnowflakeLoader) FullNameAS(self *Field, k, tableAlias string) string { // useing double underscores for uniqueness
	if self.table == nil {
		return self.Name
	}

	return fmt.Sprintf("[%s].[%s] AS %s__%s", tableAlias, self.Name, k, self.Name)
}

func (self *SnowflakeLoader) find(t *Table, others ...map[string]interface{}) (selectedFields []string, nonSelectClause string, orderby []string, limit int, offset int, err error) {
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
				fmt.Sprintf(" left join %s  [%s] on %s.[%s] = [%s].[%s] \n",
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
		nonSelectClause = fmt.Sprintf("\n %s GROUP BY %s", nonSelectClause, strings.Join(mq.GroupBy, " ,"))
	}
	//TODO install group countby then replace the select

	if len(mq.GroupCountBy) > 0 {
		if len(mq.GroupBy) > 0 {
			err = fmt.Errorf(`can not mix group and group count`)
			return
		}

		selectedFields = []string{} //clean out previous
		groupByFields := []string{}
		//TODO check if any sum/avg fucntion needed

		for _, f := range mq.GroupCountBy {

			selectedFields = append(selectedFields, f)
			if IsAggField(f) {
				continue
			}
			groupByFields = append(groupByFields, f)

		}
		selectedFields = append(selectedFields, `count(*) as count`)
		nonSelectClause = fmt.Sprintf(`%s GROUP BY %s`, nonSelectClause, strings.Join(groupByFields, " ,"))

	}
	if len(mq.SinceCountby) > 0 {

		for _, f := range mq.SinceCountby {

			field := NewSinceField(f).SelectFieldName()
			selectedFields = append(selectedFields, field)
		}
		//TODO parse since countby  createdon,day|due_date,hour
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

//replaceUnsafeFields a patch for remove unsafe field names
func (self *SnowflakeLoader) replaceUnsafeFields(t *Table, sql string) string {
	//getOperators
	if len(t.unsafeFieldsPatterns) == 0 {
		unsafeFieldsPatterns := []*pattern{}
		for _, field := range t.Fields {
			for _, op := range getOperators() {
				pat := pattern{fmt.Sprintf(` %s %s `, field.Name, op), field, op}

				unsafeFieldsPatterns = append(unsafeFieldsPatterns, &pat)
			}
		}
		t.unsafeFieldsPatterns = unsafeFieldsPatterns
	}
	for _, pat := range t.unsafeFieldsPatterns {
		toreplace := fmt.Sprintf(` %s %s `, self.FullName(pat.field), pat.operator)
		sql = strings.Replace(sql, pat.pat, toreplace, -1)
	}

	return sql
}

//ORToUnion convert or query to union
func (self *SnowflakeLoader) ORToUnion(t *Table, sql string) string {
	unions := []string{}
	or_list := []string{}
	flag := ` OR `
	if !strings.Contains(sql, flag) {
		return sql
	}

	sql_split_1 := strings.Split(sql, flag)
	if len(sql_split_1) < 2 {
		return sql
	}

	_header := strings.Split(sql_split_1[0], "(")
	if len(_header) == 0 {
		return sql
	}
	header := fmt.Sprintf(" %s (", strings.Join(_header[0:len(_header)-1], "("))
	firstOr := _header[len(_header)-1]
	or_list = append(or_list, firstOr)

	_tail := strings.Split(sql_split_1[len(sql_split_1)-1], ")")

	if len(_tail) < 2 {
		return sql
	}

	tail := fmt.Sprintf(" ) %s", strings.Join(_tail[1:], ")"))
	lastOr := _tail[0]
	or_list = append(or_list, lastOr)

	if len(sql_split_1) > 2 {

		for _, or := range sql_split_1[1 : len(sql_split_1)-1] {
			or_list = append(or_list, or)
		}

	}

	for _, or := range or_list {
		if strings.Contains(or, ` 1!=1 `) {
			continue
		}

		u := fmt.Sprintf(
			"%s %s %s\n",
			header, or, tail,
		)
		unions = append(unions, u)

	}

	return strings.Join(unions, " union \n")
}

func (self *SnowflakeLoader) FindQuery(t *Table, crit ...map[string]interface{}) (string, error) {

	//!!!below is mysql dialect by default
	selectedFields, nonSelectClause, orderby, limit, offset, err := self.find(t, crit...)
	if err != nil {
		return "", err
	}
	//TODO safe guard selectedField with square brackets
	ret := fmt.Sprintf("SELECT %s %s  \n", strings.Join(selectedFields, ", "), nonSelectClause)
	//TODO rewrite - convert OR to union query
	ret = self.ORToUnion(t, ret)
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

	//	return ret, nil

	return self.replaceUnsafeFields(t, ret), nil

}

func (self *SnowflakeLoader) CountQuery(t *Table, others ...map[string]interface{}) (string, error) {

	_, nonSelectClause, _, _, _, err := self.find(t, others...)
	if err != nil {
		return "", err
	}
	//	ret := fmt.Sprintf(`SELECT %s %s`, strings.Join(selectedFields, ", "), nonSelectClause)
	//TODO rewrite - convert OR to union query
	//	ret = self.ORToUnion(t, ret)

	return fmt.Sprintf(`SELECT count(*) as count %s`, self.replaceUnsafeFields(t, nonSelectClause)), nil

}

func (self *SnowflakeLoader) InsertQuery(t *Table, _updates map[string]interface{}) (string, error) {
	q, err := self.insertQuery(t, _updates)
	if err != nil {
		return "", err
	}

	return q, nil
}

func (self *SnowflakeLoader) MakeInsertFields(t *Table, updates []*NameVal) []string {
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

func (self *SnowflakeLoader) Escape(sql string) string {

	return strings.Replace(sql, "'", "''", -1)

}

func (self *SnowflakeLoader) InterfaceToString(i interface{}) string {

	if i == nil {
		return `null` //!!! mysql dialect
	}
	if s, ok := i.(string); ok {
		return fmt.Sprintf("'%s'", self.Escape(s))
	}
	if s, ok := i.(bool); ok {
		if s {
			return `true`
		}
		return `false`
	}

	if s, ok := i.(int); ok {
		return fmt.Sprintf(`%d`, s)
	}
	if s, ok := i.(int64); ok {
		return fmt.Sprintf(`%d`, s)
	}

	if s, ok := i.(float32); ok {
		return fmt.Sprintf(`%f`, s)
	}
	if s, ok := i.(float64); ok {
		return fmt.Sprintf(`%f`, s)
	}
	sqlTimeFormatter := "'%04d-%02d-%02d %02d:%02d:%02d'"
	if s, ok := i.(time.Time); ok {
		return fmt.Sprintf(sqlTimeFormatter, s.Year(), s.Month(), s.Day(), s.Hour(), s.Minute(), s.Second()) //TODO to be better formatted
	}

	if tPtr, ok := i.(*time.Time); ok {

		if tPtr != nil {
			s := *tPtr
			return fmt.Sprintf(sqlTimeFormatter, s.Year(), s.Month(), s.Day(), s.Hour(), s.Minute(), s.Second()) //TODO to be better formatted
		}

	}

	if IsDebug() {
		log.Println(`can not figure out type of interface `, i)
	}
	return ""
}

func (self *SnowflakeLoader) Stringify(updates map[string]interface{}) map[string]string {
	ret := make(map[string]string)

	for k, v := range updates {
		ret[k] = self.InterfaceToString(v)
	}

	return ret

}

func (self *SnowflakeLoader) insertQuery(t *Table, _updates map[string]interface{}) (string, error) {

	updates := []*NameVal{}

	for k, v := range self.Stringify(_updates) {

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

func (self *SnowflakeLoader) SafeWhere(t *Table, crit map[string]interface{}) (string, error) {
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

		if where.Protected {
			wheres = append(wheres, where)
			continue
		}
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

	whereClause := fmt.Sprintf(`WHERE 1=1  %s`, ToWhereString(t, wheres))

	return whereClause, nil

}

func (self *SnowflakeLoader) SafeUpdate(t *Table, updates map[string]interface{}) []string {
	up := []string{}
	for k, v := range self.Stringify(updates) {
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

func (self *SnowflakeLoader) UpdateQuery(t *Table, crit, updates map[string]interface{}) (string, error) {
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
func (self *SnowflakeLoader) RemoveQuery(t *Table, crit map[string]interface{}) (string, error) {
	whereClause, err := self.SafeWhere(t, crit)
	if err != nil {
		return "", err
	}
	ret := fmt.Sprintf(`DELETE FROM %s %s `, self.getTableName(t), whereClause)
	return ret, nil
}

func (self *SnowflakeLoader) GetGroupCountPage(t *Table, others ...map[string]interface{}) (*Page, error) {
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

}

func (self *SnowflakeLoader) GetGroupCountPageCount(t *Table, others ...map[string]interface{}) (int, error) {
	selectedFields, nonSelectClause, _, _, _, err := self.find(t, others...)
	if err != nil {
		return 0, err
	}
	rawQuery := fmt.Sprintf(`SELECT %s %s`, strings.Join(selectedFields, ", "), nonSelectClause)

	countQuery := fmt.Sprintf(`SELECT count(*) as count FROM (%s) temp`, rawQuery)
	if IsDebug() {
		fmt.Println(`SnowflakeLoader countQuery`, countQuery)
	}
	if IsDebug() {
		fmt.Println(countQuery)
	}
	if t.Schema == nil {
		return 0, fmt.Errorf(`no schema pointer found from table[%s]`, t.TableName)
	}
	rows, err := t.Schema.Db.Query(countQuery)
	if IsDebug() {
		fmt.Println(`SnowflakeLoader rawQuery`, rawQuery)
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

func (self *SnowflakeLoader) GetSinceCountPageTotal(t *Table, others ...map[string]interface{}) (int, error) {
	return 0, nil
}
func (self *SnowflakeLoader) GetSinceCountPageSum(t *Table, others ...map[string]interface{}) (int, error) {
	return 0, nil
}

func (self *SnowflakeLoader) GetSinceCountQuery(t *Table, others ...map[string]interface{}) (string, error) {
	mq, err := t.ParseMetaQuery(others[1])
	if err != nil {
		return "", err
	}
	if len(mq.SinceCountby) == 0 {
		return "", fmt.Errorf(`no sincecountby field ,e.g. createdon,day`)
	}
	selectedFields, nonSelectClause, _, _, _, err := self.find(t, others...)
	if err != nil {
		return "", err
	}
	rawQuery := fmt.Sprintf(`SELECT %s %s`, strings.Join(selectedFields, ", "), nonSelectClause)

	sinceField := mq.SinceCountby[0]
	//	self.ToSinceField(sinceField)
	s := NewSinceField(sinceField)
	//TODO adding where, group by, order by, offset, limit

	//making new query
	newQuery := fmt.Sprintf(
		`SELECT  %s , count(*) as count  FROM (%s) temp  WHERE 1=1`,
		s.AsName(), rawQuery,
	)

	if s.GTE != nil {
		newQuery = fmt.Sprintf(`%s AND %s >= %d `, newQuery, s.AsName(), *s.GTE)
	}

	if s.LT != nil {
		newQuery = fmt.Sprintf(`%s AND %s < %d `, newQuery, s.AsName(), *s.LT)
	}

	newQuery = fmt.Sprintf(`%s group by %s  `, newQuery, s.AsName())
	return newQuery, nil
}

func (self *SnowflakeLoader) GetSinceCountPage(t *Table, others ...map[string]interface{}) (*Page, error) {

	mq, err := t.ParseMetaQuery(others[1])
	if err != nil {
		return nil, err
	}
	sinceField := mq.SinceCountby[0]
	s := NewSinceField(sinceField)

	ret := new(Page)
	ret.Limit = t.Limit
	if mq.Limit > 0 {
		ret.Limit = mq.Limit
	}
	ret.Offset = mq.Offset
	_newQuery, err := self.GetSinceCountQuery(t, others...)
	if err != nil {
		return nil, err
	}
	//TODO do count query
	countQuery := fmt.Sprintf(`SELECT count(*) as count FROM (%s) temp1`, _newQuery)
	counts, err := t.Schema.Map(t.Schema.Db, countQuery, map[string]string{`count`: `int`})
	if err != nil {
		return nil, err
	}
	for _, _count := range counts {
		if cnt, err := ToInt(_count[`count`]); err == nil {

			ret.Total = cnt
			break
		}

	}

	newQuery := fmt.Sprintf(` 
	 
	%s 
	order by %s
	offset %d rows  Fetch next %d rows only `, _newQuery, s.AsName(), ret.Offset, ret.Limit)

	typMap := make(map[string]string)
	typMap[s.AsName()] = `int`
	typMap[`count`] = `int`
	founds, err := t.Schema.Map(t.Schema.Db, newQuery, typMap)
	if err != nil {
		return nil, err
	}
	for _, found := range founds {
		if cnt, err := ToInt(found[`count`]); err == nil {
			ret.Sum += cnt
		}
	}
	ret.Data = founds
	//calculate summation
	return ret, nil
}
