package querybuilder

import (
	//	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var (
	AtomicKinds = []reflect.Kind{
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Uintptr,
		reflect.Float32,
		reflect.Float64,
		reflect.String,
		reflect.Bool,
	}
	StructExceptions = []string{
		`Time`,
	}

	Operators = []string{
		"$eq",
		"$gt",
		"$gte",
		"$lt",
		"$lte",
		"$ne",
		"$in",
		"$nin",
		"$exists",
		"$regex",
		"$like",
	}
	ArrayValOps = []string{
		"$in",
		"$nin",
	}
)

type QueryParams struct {
	Limit        int
	Skip         int
	Fields       map[string]int `json:"-"`
	Or           bool
	Critiera     map[string]interface{}
	SortBy       []string
	GroupBy      []string
	GroupCountBy []string
	SinceCountBy []string
	Populates    []string //simple form for left join
}

func IsAtomicKinds(k reflect.Kind) bool {
	for _, _k := range AtomicKinds {
		if _k == k {
			return true
		}
	}

	return false
}

func IsExceptionedStruct(n string) bool {
	for _, _k := range StructExceptions {
		if _k == n {
			return true
		}
	}

	return false
}

func IsOperatorAllowed(op string) bool {
	for _, _op := range Operators {
		if _op == op {
			return true
		}
	}
	return false
}

func NeedArrayVals(op string) bool {

	for _, _op := range ArrayValOps {
		if _op == op {
			return true
		}
	}
	return false
}

//kType is only int, float, string or Time

func BuildOne(kType string, text string, needOr bool, m map[string]interface{}) error {

	//e.g field = $in:va1,val2,val3
	opVal := strings.SplitN(text, ":", 3)

	//!!!default only one item len(opVal) == 1
	isOr := false
	op := `$eq`
	vals := text

	if len(opVal) == 0 {
		return fmt.Errorf("missing op or value from %s", text)
	}

	if len(opVal) >= 3 {
		if opVal[0] == "or" { //!!!ignore if anything else? default is $and
			isOr = true
		}
		op = opVal[1]
		vals = opVal[2]
	}
	if len(opVal) == 2 {
		op = opVal[0]
		vals = opVal[1]
	}

	if (needOr && !isOr) || (isOr && !needOr) {
		return nil //!!! skip done
	}

	if text == `$exists` {

		m[text] = ""

		return nil
	}

	//validate op
	if !IsOperatorAllowed(op) {
		m[`$eq`] = text // treat it as a single equal operator
		return nil
		//		return fmt.Errorf("%s is not allowed to use", op)
	}

	vals = strings.Trim(vals, " ")
	if vals == "" {
		return fmt.Errorf(`no value found ` + text)
	}
	if op == `$exists` {
		m[op] = false
		if vals == `true` {
			m[op] = true
		}

		return nil
	}

	//insert {op:val}
	if !NeedArrayVals(op) {
		m[op] = vals

		switch kType {
		case `Time`:
			if len(vals) == 10 {
				vals = fmt.Sprintf("%sT00:00:00-07:00", vals)
			}
			t, err := time.Parse(time.RFC3339, vals)
			if err != nil {
				return fmt.Errorf("  %s  ", err.Error())
			}
			m[op] = t
		case `int`:
			m[op], _ = strconv.Atoi(vals)
		case `int64`:
			m[op], _ = strconv.ParseInt(vals, 10, 64)
		case `float64`:
			m[op], _ = strconv.ParseFloat(vals, 64)
		case `float32`:
			if f32, err := strconv.ParseFloat(vals, 64); err == nil {
				m[op] = float32(f32)
			}
		}
		return nil
	}
	//build a list of arrays
	values := []interface{}{}
	splits := strings.Split(vals, ",")
	for _, v := range splits {
		var _v interface{}
		_v = v
		switch kType {
		case `string`:
			_v = v
		case `int`:
			_v, _ = strconv.Atoi(v)
		case `int64`:
			_v, _ = strconv.ParseInt(v, 10, 64)
		case `float64`:
			_v, _ = strconv.ParseFloat(v, 64)
		case `float32`:
			if f32, err := strconv.ParseFloat(v, 64); err == nil {
				_v = float32(f32)
			}
		}
		values = append(values, _v)
	}

	m[op] = values

	//TODO type casting
	return nil
}

func BuildOneParam(kType string, text string, andMap map[string]interface{}, orMap map[string]interface{}) error {

	//e.g field = $in:va1,val2,val3|$lt:val
	ops := strings.Split(text, "|")

	for _, op := range ops {

		if err := BuildOne(kType, op, false, andMap); err != nil {
			return err
		}
		//		fmt.Println(op, ops)
		if err := BuildOne(kType, op, true, orMap); err != nil {
			return err
		}
	}
	return nil
}

func FieldType(f string, fieldMap map[string]string) string {

	if v, ok := fieldMap[strings.ToLower(f)]; ok {
		return v
	}
	return ``
}

//Build need pull all known fields
//TODO parse limit, offset, selected fields

func BuildAllParams(params map[string]string, fieldMap map[string]string) (map[string]interface{}, error) {
	//query :=`k1=`
	//TODO analyze fields
	//compare with params

	//	fieldMap := GetNonStructFields(i)
	m := make(map[string]interface{})
	var OrList []interface{}
	for k, v := range params {
		t := FieldType(k, fieldMap)

		andMap := make(map[string]interface{})
		orMap := make(map[string]interface{})
		if err := BuildOneParam(t, v, andMap, orMap); err != nil {
			return nil, err
		}
		if len(orMap) > 0 {
			_m := make(map[string]interface{})
			_m[k] = orMap
			OrList = append(OrList, _m)
		}

		if len(andMap) > 0 {
			m[k] = andMap
		}
	}

	if len(OrList) == 0 {
		return m, nil
	}
	//build compound query

	//{
	//    $and: [
	//            { status:'closed'},
	//            {
	//                "$or":[{pgbarcode:{$regex:"PG.*"}}]
	//            }
	//          ]
	//}
	ret := make(map[string]interface{})
	var and []interface{}

	or := make(map[string]interface{})
	or[`$or`] = OrList
	for k, v := range m {
		_m := make(map[string]interface{})
		_m[k] = v
		and = append(and, _m)
	}

	and = append(and, or)
	ret[`$and`] = and
	return ret, nil
}

type CanGet interface {
	Get(string) string
}

func Build(c CanGet, fieldMap map[string]string) (*QueryParams, error) {
	ret := &QueryParams{
		Limit:  30,
		Fields: make(map[string]int),
	}

	critMap := make(map[string]string)
	for k, _ := range fieldMap {
		ck := c.Get(k)
		if ck == "" {
			continue
		}

		critMap[k] = ck
	}

	var err error

	ret.Critiera, err = BuildAllParams(critMap, fieldMap)
	if err != nil {
		return nil, err
	}
	if limit := c.Get(`_limit`); limit != "" {
		ret.Limit, err = strconv.Atoi(limit)
		if err != nil {
			return nil, fmt.Errorf(`parse Limit error ` + err.Error())
		}
	}
	if skip := c.Get(`_skip`); skip != "" {
		ret.Skip, err = strconv.Atoi(skip)
		if err != nil {
			return nil, fmt.Errorf(`parse skip error ` + err.Error())
		}
	}
	if sortBy := c.Get(`_sortby`); sortBy != "" {
		ret.SortBy = strings.Split(sortBy, ",")

	}

	if groupCountBy := c.Get(`_groupby`); groupCountBy != "" {
		ret.GroupBy = strings.Split(groupCountBy, ",")

	}
	if groupCountBy := c.Get(`_groupcountby`); groupCountBy != "" {
		ret.GroupCountBy = strings.Split(groupCountBy, ",")

	}
	if sinceCountBy := c.Get(`_sincecountby`); sinceCountBy != "" {
		ret.SinceCountBy = strings.Split(sinceCountBy, "|")

	}

	if groupCountBy := c.Get(`_populates`); groupCountBy != "" {
		ret.Populates = strings.Split(groupCountBy, "|")
	}

	if fields := c.Get(`_fields`); fields != "" {
		fs := strings.Split(fields, ",")
		ret.Fields = make(map[string]int)
		for _, f := range fs {
			if _, ok := fieldMap[f]; !ok {
				return nil, fmt.Errorf(`%s is not in allowed field type map `, f)
			}
			ret.Fields[f] = 1
		}

		//default to supress all sub document

		if err != nil {
			return nil, fmt.Errorf(`parse fields error ` + err.Error())
		}
	}
	if len(ret.Fields) == 0 {
		for k, _ := range fieldMap {
			ret.Fields[k] = 1
		}
	}
	//	b, _ := json.MarshalIndent(ret, "", "    ")
	//	fmt.Println(string(b))
	return ret, nil
}
