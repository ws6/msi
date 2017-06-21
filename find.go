package msi

import (
	"fmt"
	"strings"
	"time"
)

//crit can be nested crit
//if crit.key is a field, using =
//if crit.key is any $and, $or,$ne, $nin, $in, $gt, $gte, $lt, $lte, then translate it to sub query
//only try one level

const (
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
	NE  = `$ne`
	//range operators
	IN  = `$in`  //need values
	NIN = `$nin` //need values
	//existence operators
	EXISTS = `$exists` //if value is true then field is not null; else field is null
	LIKE   = `$like`
)

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
	case NE:
		return `!=`
	case IN:
		return `IN`
	case NIN:
		return `NOT IN`
	case EXISTS:
		return `IS NOT NULL`
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

	//build select query

	_wheres, err := ParseCrit(crit)
	if err != nil {
		return "", err
	}

	wheres := []*Where{}

	for _, where := range _wheres {
		//Align fieldname
		for _, field := range t.Fields {
			if field.Name == where.FieldName {
				wheres = append(wheres, where)
			}
		}
	}
	whereClause := fmt.Sprintf(`WHERE TRUE  %s`, ToWhereString(wheres))

	return whereClause, nil

}

//Find filter out bad fields and correct types to make query
func (t *Table) Find(crit map[string]interface{}) (string, error) {
	selectedFields := []string{}
	for _, field := range t.Fields {
		if field.Selected {
			selectedFields = append(selectedFields, field.Name)
		}
	}

	if len(selectedFields) == 0 {
		selectedFields = []string{`*`} //not sufficient
	}
	ret := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectedFields, ", "), t.TableName)
	if crit == nil {
		return fmt.Sprintf("%s  ", ret), nil
	}

	whereClause, err := t.SafeWhere(crit)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`%s %s `, ret, whereClause), nil
}

func (t *Table) FindId(id int) (string, error) {
	return t.Find(map[string]interface{}{`id`: id})
}
