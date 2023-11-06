package querybuilder

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ws6/msi"
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
		"$null",
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
	Populates2   []string //verision 2 of populates
	OutCountBy   []string //added 2023, counting how many refereces rows from fk table
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
	opVal := strings.SplitN(text, ":", 2)

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

	if text == `$exists` || text == `$null` {

		m[text] = true

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
	if op == `$exists` || op == `$null` {

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
		var err error
		switch kType {
		case `string`:
			_v = v
		case `int`:
			_v, err = strconv.Atoi(v)
			if err != nil {
				_v = v
			}
		case `int64`:
			_v, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				_v = v
			}
		case `float64`:
			_v, err = strconv.ParseFloat(v, 64)
			if err != nil {
				_v = v
			}
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
		// fmt.Println(`op===>`, op)
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

//MapUpdater interface for key/value operations
type MapUpdater interface {
	SetTypeMap(string, string)
	GetTypeMap() map[string]string
}

//TableLoader interface for connect MapUpdater and CanPopulate2
type TableLoader interface {
	GetTable() *msi.Table
	GetLoader() msi.ShemaLoader
}

//CanPopulate2 interface can build special feature -CanPopulate2
type CanPopulate2 interface {
	CompileAllPopulates2(t *msi.Table, populates2 []string) (
		selectedFields []string,
		nonSelectClause []string,
		typeMap map[string]string,
		err error,
	)
}

func (self *QueryParams) BuildOutcountBy(outCountBy string, mu MapUpdater) error {
	self.OutCountBy = strings.Split(outCountBy, "|")
	//inject typeMap

	for _, f := range self.OutCountBy {
		outCountByFieldName := fmt.Sprintf(`%s__outcount`, f) //facilitate table typeMap
		mu.SetTypeMap(outCountByFieldName, `int64`)
	}
	return nil
}
func (self *QueryParams) BuildPopulate2(populate2 string, mu MapUpdater) error {

	self.Populates2 = strings.Split(populate2, "|")

	tu, ok := mu.(TableLoader)
	if !ok {
		fmt.Println(`!!!not TableLoader`)
		return nil
	}
	loader := tu.GetLoader()
	if loader == nil {
		return fmt.Errorf(`loader is empty`)
	}
	p2, ok := loader.(CanPopulate2)
	if !ok {
		fmt.Println(`!!!not CanPopulate2`)
		return nil
	}
	_, _, typeMap, err := p2.CompileAllPopulates2(tu.GetTable(), self.Populates2)
	if err != nil {
		return err
	}

	for k, v := range typeMap {
		mu.SetTypeMap(k, v)
	}
	return nil
}

func Build(c CanGet, mu MapUpdater) (*QueryParams, error) {
	ret := &QueryParams{
		Limit:  30,
		Fields: make(map[string]int),
	}
	var err error

	if outCountBy := c.Get(`_outcountby`); outCountBy != "" {
		if err := ret.BuildOutcountBy(outCountBy, mu); err != nil {
			return nil, fmt.Errorf(`BuildOutcountBy:%s`, err.Error())
		}
	}

	if populate2 := c.Get(`_populates2`); populate2 != "" {
		if err := ret.BuildPopulate2(populate2, mu); err != nil {
			return nil, fmt.Errorf(`BuildPopulate2:%s`, err.Error())
		}
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

	//building Crit at last because typeMap are still get updatting
	if true {

		critMap := make(map[string]string)
		fieldMap := mu.GetTypeMap()

		for k, _ := range fieldMap {
			ck := c.Get(k)
			if ck == "" {
				continue
			}

			critMap[k] = ck
		}

		ret.Critiera, err = BuildAllParams(critMap, fieldMap)
		if err != nil {
			return nil, err
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
	}
	return ret, nil
}
