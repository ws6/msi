package msi

import (
	"fmt"
	"log"
	//	"reflect"
	"strings"
	"time"
)

//crit can be nested crit
//if crit.key is a field, using =
//if crit.key is any $and, $or,$ne, $nin, $in, $gt, $gte, $lt, $lte, then translate it to sub query
//only try one level

const (
	IS_NOT_NULL = `IS NOT NULL`
	UNLIMIT     = -1
	//logical operators
	AND = `$and`
	OR  = `$or`
	XOR = `$xor`
	//compute operators
	GT  = `$gt`
	LT  = `$lt`
	GTE = `$gte`
	LTE = `$lte`
	EQ  = `$eq`
	IS  = `$is`
	NE  = `$ne`
	//range operators
	IN  = `$in`  //need values
	NIN = `$nin` //need values
	//existence operators
	EXISTS = `$exists` //if value is true then field is not null; else field is null
	LIKE   = `$like`
	//meta query constants
	FIELDS       = `$fields`    // not part of SQL syntax; for overwritting the default field selection
	JOINS        = `$joins`     // not part of SQL syntax
	POPULATES    = `$populates` // not part of SQL syntax; accept array of strings; if each field has foreign fields specified will use other wise, use all foreign fields  ["field1:foreign_field1,foreign_field2", "field2" ]
	OFFSET       = `$offset`
	LIMIT        = `$limit`
	GROUPBY      = `$groupby`
	GROUPCOUNTBY = `$groupcountby`

	ORDERBY = `$orderby`
)

func IsMetaQuery(op string) bool {
	switch op {
	default:
		return false
	case FIELDS:
		return true
	case OFFSET:
		return true
	case LIMIT:
		return true
	case GROUPBY:
		return true
	case GROUPCOUNTBY:
		return true
	case ORDERBY:
		return true
	case JOINS:
		return true
	case POPULATES:
		return true
	}
	return false
}

func ToSQLOperator(op string) string {
	switch op {
	default:
		return ""
	case AND:
		return `AND`
	case OR:
		return `OR`
	case XOR:
		return `XOR`
	case GT:
		return `>`
	case LT:
		return `<`
	case GTE:
		return `>=`
	case LTE:
		return `<=`
	case EQ:
		return `=`
	case IS:
		return `is`
	case NE:
		return `!=`
	case IN:
		return `IN`
	case NIN:
		return `NOT IN`
	case EXISTS:
		return IS_NOT_NULL
	case LIKE:
		return `LIKE`
	}
	return ""
}

func IsLogicOperator(op string) bool {

	if op == AND || op == OR || op == XOR {
		return true
	}

	return false
}

func needMultipleValues(op string) bool {
	switch op {
	default:
		return false
	case IN:
		return true
	case NIN:
		return true
	}
	return false
}

func IsComputeOperator(op string) bool {
	switch op {
	default:
		return false
	case NE:
		return true
	case GT:
		return true
	case LT:
		return true
	case GTE:
		return true
	case LTE:
		return true
	case EQ:
		return true
	case IN:
		return true
	case NIN:
		return true
	case EXISTS:
		return true
	case LIKE:
		return true
	}
	return false
}

func isOperator(op string) bool {
	return IsLogicOperator(op) || IsComputeOperator(op)
}

type Where struct {
	LogicOperator string //https://dev.mysql.com/doc/refman/5.7/en/logical-operators.html
	Operator      string //https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html
	FieldName     string // actual field name
	Value         interface{}
}

func NewWhere(logicOp, compOp string) *Where {
	ret := new(Where)
	ret.LogicOperator = AND
	if logicOp != "" {
		ret.LogicOperator = logicOp
	}

	ret.Operator = EQ

	if compOp != "" {
		ret.Operator = compOp
	}

	return ret
}

type WhereErr struct {
	Message string
}

func ParseCritErr(err interface{}) bool {
	if _, ok := err.(WhereErr); ok {
		return true
	}
	return false
}

func (self *WhereErr) Error() string {
	return self.Message
}

func itos(i interface{}) string {
	if i == nil {
		return ``
	}
	switch v := i.(type) {
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%f", v)
	case float32:
		return fmt.Sprintf("%f", v)
	case string:
		return fmt.Sprintf("'%s'", v)
	case bool:
		if v {
			return `true`
		}
		return `false`
	case time.Time:
		return v.String()
	case *time.Time:
		return v.String()
	}

	return ""
}

func ToArray(i interface{}) []string {

	ret := []string{}

	if ints, ok := i.([]int); ok {
		for _, v := range ints {
			ret = append(ret, fmt.Sprintf("%d", v))
		}
		return ret
	}

	if ints, ok := i.([]int64); ok {
		for _, v := range ints {
			ret = append(ret, fmt.Sprintf("%d", v))
		}
		return ret
	}

	if ints, ok := i.([]float32); ok {
		for _, v := range ints {
			ret = append(ret, fmt.Sprintf("%f", v))
		}
		return ret
	}
	if ints, ok := i.([]string); ok {
		for _, v := range ints {
			ret = append(ret, fmt.Sprintf("'%s'", Escape(v)))
		}
		return ret
	}
	if ints, ok := i.([]bool); ok {
		for _, _v := range ints {
			v := `false`
			if _v {
				v = `true`
			}
			ret = append(ret, v)
		}
		return ret
	}

	if ints, ok := i.([]time.Time); ok {
		for _, v := range ints {

			ret = append(ret, v.String())
		}
		return ret
	}
	if ints, ok := i.([]*time.Time); ok {
		for _, v := range ints {

			ret = append(ret, v.String())
		}
		return ret
	}

	if s, ok := i.([]interface{}); ok {
		for _, _i := range s {
			ret = append(ret, itos(_i))
		}
		return ret
	}

	return ret
}

func (w *Where) Values() []string {

	if needMultipleValues(w.Operator) {

		return ToArray(w.Value)
	}

	return []string{InterfaceToString(w.Value)}

}

func (w *Where) GetValueString() string {
	values := w.Values()

	if needMultipleValues(w.Operator) {
		return fmt.Sprintf("(%s)", strings.Join(values, ", "))
	}
	if len(values) > 0 {
		return values[0]
	}
	return ""
}

func (w *Where) String() string {
	//TODO print values
	if w.Operator == EQ {

		if w.GetValueString() == fmt.Sprintf("'%s'", EXISTS) {
			return fmt.Sprintf(`%s %s %s`, ToSQLOperator(w.LogicOperator), w.FieldName, ToSQLOperator(EXISTS))
		}
	}

	if w.Operator == LIKE {
		v := fmt.Sprintf(`%s`, w.Value)
		//replacing special chars __ to %
		v = strings.Replace(v, "__", "%", -1)

		ret := fmt.Sprintf(`%s %s %s '%s'`,
			ToSQLOperator(w.LogicOperator),
			w.FieldName,
			ToSQLOperator(w.Operator),
			v,
			//			w.Value,
		)
		return ret
	}

	if w.Operator == EXISTS {
		return fmt.Sprintf(`%s %s %s`,
			ToSQLOperator(w.LogicOperator),
			w.FieldName,
			ToSQLOperator(w.Operator),
		)
	}

	ret := fmt.Sprintf(`%s %s %s %s`,
		ToSQLOperator(w.LogicOperator),
		w.FieldName,
		ToSQLOperator(w.Operator),
		w.GetValueString(),
	)

	return ret
}

func ToWhereString(wheres []*Where) string {
	ret := []string{}
	for _, w := range wheres {
		ret = append(ret, w.String())
	}
	return strings.Join(ret, " ")
}

func ParseCrit(crit map[string]interface{}) ([]*Where, error) {

	ret := []*Where{}
	err := new(WhereErr)
	ParseWhere(crit, &ret, "", "", "", err)

	if err.Message != "" {
		return nil, fmt.Errorf(err.Error())
	}
	return ret, nil
}

func ParseWhere(crit map[string]interface{}, ret *[]*Where, logicOp, compOp, fieldname string, err *WhereErr) {

	if err.Message != "" {
		return
	}
	if ret == nil {
		err.Message = fmt.Sprintf(`storage parameter "ret"is nil`)
		return
	}

	for k, v := range crit {

		if _crit, ok := v.(map[string]interface{}); ok {

			_logicOp := logicOp
			if IsLogicOperator(k) {
				_logicOp = k
			}
			_compOp := compOp
			if IsComputeOperator(k) {
				_compOp = k
			}

			_fieldname := fieldname
			if !isOperator(k) {
				_fieldname = k
			}

			ParseWhere(_crit, ret, _logicOp, _compOp, _fieldname, err)
			continue
		}

		where := NewWhere(logicOp, compOp)

		if v == nil {
			if where.Operator == EQ {
				where.Operator = IS // switching the IS operator
			}
		}

		where.FieldName = k
		if isOperator(k) {
			where.Operator = k
			where.FieldName = fieldname
		}
		if where.FieldName == "" {
			err.Message = fmt.Sprintf(`no field name found at where v=%+v`, v)
			return
		}
		if isOperator(where.FieldName) {
			err.Message = fmt.Sprintf(`wrong usage on fieldname[%s], it is defined as opertor`, where.FieldName)
			return
		}
		isArray := IsArray(v)
		//TODO sanity check according compute operator
		if needMultipleValues(where.Operator) && !isArray {
			err.Message = fmt.Sprintf("value is not array like[%v]", v)
			return
		}

		if !needMultipleValues(where.Operator) && isArray {
			err.Message = fmt.Sprintf("value is  array like[%v], operator[%s] doesnt want", v, where.Operator)
			return
		}

		where.Value = v

		*ret = append(*ret, where)

	}

}

func (t *Table) SafeWhere(crit map[string]interface{}) (string, error) {
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
			if DEBUG {
				log.Println(`fk is allowed`, where.FieldName)
			}
			wheres = append(wheres, where)
			continue
		}
		where.FieldName = fmt.Sprintf(`%s.%s`, t.TableName, where.FieldName)
		for _, field := range t.Fields {
			//loosing the checker by allow tablename.fieldname format
			if field.Name == where.FieldName || fmt.Sprintf(`%s.%s`, t.TableName, field.Name) == where.FieldName {
				wheres = append(wheres, where)
				continue
			}
			//			if DEBUG {
			//				log.Println(`where fieldname get filtered `, where.FieldName, t.TableName)
			//			}
		}
	}
	whereClause := fmt.Sprintf(`WHERE TRUE  %s`, ToWhereString(wheres))

	return whereClause, nil

}

type MetaQuery struct {
	Orderby      []string
	Offset       int
	Limit        int
	GroupBy      []string
	GroupCountBy []string //special grouping will replace select fields
	Fields       []string
	Joins        []string
	Populates    []string
}

func InterfaceToStringArray(v interface{}) []string {
	ret := []string{}

	arr, ok := v.([]string)
	if !ok {
		return ret
	}

	for _, s := range arr {
		ret = append(ret, s)
	}

	return ret
}

func (t *Table) ParseMetaQuery(crit map[string]interface{}) (*MetaQuery, error) {

	if crit != nil {

		return ParseMetaQuery(crit)
	}

	ret := new(MetaQuery)
	ret.Limit = t.Limit

	return ret, nil

}

func ParseMetaQuery(crit map[string]interface{}) (*MetaQuery, error) {
	ret := new(MetaQuery)
	for k, v := range crit {
		if !IsMetaQuery(k) {
			continue
		}
		switch k {
		case OFFSET:
			if n, ok := v.(int); ok {
				ret.Offset = n
			}

		case LIMIT:
			n, ok := v.(int)
			if ok {
				ret.Limit = n
			}

		case GROUPBY:
			ret.GroupBy = InterfaceToStringArray(v)
		case GROUPCOUNTBY:
			ret.GroupCountBy = InterfaceToStringArray(v)

		case FIELDS:
			ret.Fields = InterfaceToStringArray(v)
		case ORDERBY:
			ret.Orderby = InterfaceToStringArray(v)
		case JOINS:
			ret.Joins = InterfaceToStringArray(v)
			//TODO support auto join $join:[{tablename:[selected_forgein_table_fields]}]
		case POPULATES:
			ret.Populates = InterfaceToStringArray(v)
		default:
			continue
		}
	}
	return ret, nil
}

//func getTableAlias(referencedTableName, thisFieldName string) string {
//	return fmt.Sprintf("%s__%s", referencedTableName, thisFieldName)
//}

func (t *Table) find(others ...map[string]interface{}) (selectedFields []string, nonSelectClause string, orderby []string, limit int, offset int, err error) {
	for _, field := range t.Fields {
		if field.Selected {
			selectedFields = append(selectedFields, field.FullName())
		}
	}

	defer func() {
		if len(selectedFields) == 0 {
			selectedFields = []string{`*`} //not sufficient
		}
	}()

	getTableName := func() string {
		if t.Schema == nil {

			return t.TableName
		}

		return fmt.Sprintf("%s.%s",
			t.Schema.DatabaseName,
			t.TableName)
	}

	nonSelectClause = fmt.Sprintf("FROM %s",
		getTableName(),
	)

	if len(others) == 0 {
		return
	}

	var crit map[string]interface{}

	if len(others) > 0 {
		crit = others[0]
	}

	whereClause, _err := t.SafeWhere(crit)
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

	if DEBUG {
		log.Printf("%+v", mq)
	}

	if len(mq.Fields) > 0 {
		selectedFields = mq.Fields //!!!overwrite
	}

	nonSelectClause = fmt.Sprintf("FROM %s  ", getTableName())

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
					field.ReferencedTable.TableName,
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
					foreignFields = append(foreignFields, foreignField.FullNameAS(fieldName, tableAlias))
				}
			}

			if len(foreignFields) == 0 {
				//use all foreign table fields
				for _, field := range field.ReferencedTable.Fields {

					if field.Hide == true {
						continue
					}

					foreignFields = append(foreignFields, field.FullNameAS(fieldName, tableAlias))
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

//Find filter out bad fields and correct types to make query
func (t *Table) FindQuery(others ...map[string]interface{}) (string, error) {

	selectedFields, nonSelectClause, orderby, limit, offset, err := t.find(others...)
	if err != nil {
		return "", err
	}
	ret := fmt.Sprintf(`SELECT %s %s`, strings.Join(selectedFields, ", "), nonSelectClause)

	//install orderby
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

//Count the field name out of this query is "count" in lowercase
func (t *Table) CountQuery(others ...map[string]interface{}) (string, error) {

	_, nonSelectClause, _, _, _, err := t.find(others...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`SELECT count(*) as count %s`, nonSelectClause), nil
}
