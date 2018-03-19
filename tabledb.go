package msi

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	NOT_FOUND = fmt.Errorf(`not found`)
)

func IsNotFoundErr(err error) bool {
	return err == NOT_FOUND
}

type Stmt struct {
	query  string
	Db     *sql.DB
	err    error
	total  int
	others []map[string]interface{}
	table  *Table
}

//Page one page of results with Total count information
type Page struct {
	Total int

	Limit  int
	Offset int

	FindErr  error
	CountErr error
	Data     []map[string]interface{}
}

////selectedFields, nonSelectClause, orderby, limit, offset, err := t.find(others...)
//	if err != nil {
//		return "", err
//	}

//GetGroupCountPage get total count, sum , and actual page
func (self *Table) GetGroupCountPageCount(others ...map[string]interface{}) (int, error) {
	selectedFields, nonSelectClause, _, _, _, err := self.find(others...)
	if err != nil {
		return 0, err
	}
	rawQuery := fmt.Sprintf(`SELECT %s %s`, strings.Join(selectedFields, ", "), nonSelectClause)

	//re-wrap it into a new statement

	//	https://stackoverflow.com/questions/14880080/mysql-sum-of-a-count-with-group-by-clause

	countQuery := fmt.Sprintf(`SELECT count(*) as count FROM (%s) temp`, rawQuery)
	fmt.Println(`countQuery`, countQuery)
	if DEBUG {
		fmt.Println(countQuery)
	}
	if self.Schema == nil {
		return 0, fmt.Errorf(`no schema pointer found from table[%s]`, self.TableName)
	}
	rows, err := self.Schema.Db.Query(countQuery)
	fmt.Println(`rawQuery`, rawQuery)
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

func (self *Table) GetGroupCountPage(others ...map[string]interface{}) (*Page, error) {

	if dl, ok := self.Schema.loader.(Dialect); ok {
		res, err := dl.GetGroupCountPage(self, others...)
		if err == nil {
			return res, nil
		}

		if err != ERR_USE_MYSQL {
			return nil, err
		}
	}

	ret := new(Page)
	ret.Limit = self.Limit
	fmt.Println(`<- find ->GetGroupCountPageCount`)
	_, _, _, limit, offset, err := self.find(others...)
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
		fmt.Println(`Find ->GetGroupCountPageCount`)
		ret.Data, ret.FindErr = self.Find(others...).Map(map[string]string{`count`: `int`})
		_wg.Done()
	}(&wg)

	go func(_wg *sync.WaitGroup) {
		fmt.Println(`GetGroupCountPageCount`)
		ret.Total, ret.CountErr = self.GetGroupCountPageCount(others...)
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

func (self *Table) GetPage(others ...map[string]interface{}) (*Page, error) {

	ret := new(Page)
	ret.Limit = self.Limit
	//TODO others get analyzed three times. need to re-used the analyzed results
	_, _, _, limit, offset, err := self.find(others...)
	if err != nil {
		return nil, err
	}

	if limit != 0 {
		ret.Limit = limit
	}

	ret.Offset = offset

	var wg sync.WaitGroup

	wg.Add(2)
	//This is why we Go, isn't it?
	go func(_wg *sync.WaitGroup) {
		ret.Data, ret.FindErr = self.Find(others...).Map()
		_wg.Done()
	}(&wg)
	go func(_wg *sync.WaitGroup) {
		ret.Total, ret.CountErr = self.Find(others...).Count()
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

func (self *Table) Find(others ...map[string]interface{}) *Stmt {
	//install configurations

	ret := new(Stmt)

	if self.Schema != nil && self.Schema.LifeCycle != nil {
		for _, f := range self.Schema.LifeCycle.BeforeFinds {
			if err := f(others...); err != nil {
				ret.err = err
			}

		}
	}

	if self.LifeCycle != nil {
		for _, f := range self.LifeCycle.BeforeFinds {
			if err := f(others...); err != nil {
				ret.err = err
			}

		}
	}
	ret.Db = self.Schema.Db
	ret.others = others
	ret.table = self
	ret.total = -1
	return ret
}

func (s *Stmt) ClosableChan(limit int, done <-chan bool) chan map[string]interface{} {
	ret := make(chan map[string]interface{}, limit*3) //!!! three times bigger than limit

	metaQuery := map[string]interface{}{
		LIMIT: limit, OFFSET: 0,
	}
	if len(s.others) == 0 {
		s.others = append(s.others, nil)
	}

	if len(s.others) == 1 {
		s.others = append(s.others, metaQuery)
	}

	if _, ok := s.others[1][LIMIT]; !ok {
		s.others[1][LIMIT] = limit
	}
	if _, ok := s.others[1][OFFSET]; !ok {
		s.others[1][OFFSET] = 0
	}

	go func() {
		offset, ok := s.others[1][OFFSET].(int)
		if !ok {
			offset = 0
		}
		defer close(ret)

		for {
			results, err := s.Map()
			offset += limit
			s.others[1][OFFSET] = offset

			if err != nil {
				if DEBUG {
					log.Println(err.Error())
				}
				break
			}

			if len(results) == 0 {
				break
			}

			for _, result := range results {
				select {
				case ret <- result:
					continue
				case <-done:
					return
				}
			}
		}

	}()

	return ret
}

func (s *Stmt) Chan(limit int) chan map[string]interface{} {
	ret := make(chan map[string]interface{}, limit*3) //!!! three times bigger than limit

	metaQuery := map[string]interface{}{
		LIMIT: limit, OFFSET: 0,
	}
	if len(s.others) == 0 {
		s.others = append(s.others, nil)
	}

	if len(s.others) == 1 {
		s.others = append(s.others, metaQuery)
	}

	if _, ok := s.others[1][LIMIT]; !ok {
		s.others[1][LIMIT] = limit
	}
	if _, ok := s.others[1][OFFSET]; !ok {
		s.others[1][OFFSET] = 0
	}

	go func() {
		offset, ok := s.others[1][OFFSET].(int)
		if !ok {
			offset = 0
		}
		defer close(ret)

		for {
			results, err := s.Map()
			offset += limit
			s.others[1][OFFSET] = offset

			if err != nil {
				if DEBUG {
					log.Println(err.Error())
				}
				break
			}

			if len(results) == 0 {
				break
			}

			for _, result := range results {
				ret <- result
			}
		}

	}()

	return ret
}

func (s *Stmt) Count() (int, error) {
	if s.err != nil {
		return 0, s.err
	}

	query, err := s.table.CountQuery(s.others...)
	if err != nil {
		return 0, err
	}
	if DEBUG {
		log.Println(query)
	}
	rows, err := s.Db.Query(query)

	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var total int
	for rows.Next() {
		err := rows.Scan(&total)
		if err != nil {
			return 0, err
		}
		s.total = total
		return total, nil
		break
	}

	return 0, nil
}

func GetTyped(destType string, i interface{}) interface{} {
	if i == nil {
		return "nil"
	}
	switch i.(type) {
	default:
		return "unknown"
	case int64:
		return "int64"
	case float32:
		return "float32"
	case float64:
		return "float64"
	case bool:
		return "bool"
	case string:
		return "string"
	case time.Time:
		return "time.Time"
	case []byte:
		return "[]byte"
	}

	return "unknown"
}

func (t *Table) GetMyField(colName string) *Field {
	for _, f := range t.Fields {
		if f.Name == colName {
			return f
		}
	}
	return nil
}

func (t *Table) GetField(colName string) *Field {

	for _, f := range t.Fields {
		if f.Name == colName || fmt.Sprintf("%s.%s", t.TableName, f.Name) == colName {
			return f
		}
	}

	//digging up to entire database

	for _, table := range t.Schema.Tables {
		for _, f := range table.Fields {
			if f.Name == colName || fmt.Sprintf("%s.%s", table.TableName, f.Name) == colName {
				return f
			}
		}
	}

	return nil
}

func ParseByte(_type string, b []byte) interface{} {

	sb := string(b)
	switch _type {
	default:
		if DEBUG {
			log.Panicln(`unsupported type`, _type, string(b))
		}
		return b
	case `int`:
		n, _ := strconv.Atoi(sb)
		return n
	case `int64`:
		n, _ := strconv.ParseInt(sb, 10, 64)
		return n
	case `float32`:
		n, _ := strconv.ParseFloat(sb, 32)
		return float32(n)
	case `float64`:
		n, _ := strconv.ParseFloat(sb, 64)
		return float64(n)
	case `string`:

		return sb
	case `bool`:
		return sb == `true` || sb == `1`
	case `time.Time`:
		{

			formats := []string{`2006-01-02 15:04:05`, `2006-01-02`}
			var err error
			var t time.Time
			for _, format := range formats {
				_sb := sb
				//				formatLen := len(format)
				//				if len(sb) >= formatLen {
				//					_sb = sb[0 : formatLen-1]
				//				}
				if USE_LOCAL {
					t, err := time.ParseInLocation(format, _sb, time.Local)
					if err == nil {
						return t
					}
					if err != nil {
						if DEBUG {
							log.Println(`wrong time formatter`, _type, sb, err.Error())
						}

					}
				}

				t, err = time.Parse(format, _sb)

				if err == nil {
					//					if USE_LOCAL {
					//						return t.Local()
					//					}
					return t
				}

			}
			if err != nil {
				if DEBUG {
					log.Println(`wrong time formatter`, _type, sb, err.Error())
				}

			}
		}

	}

	return b
}

func ParseVal(_type string, v interface{}) interface{} {
	//	if f == nil {
	//		return nil
	//	}

	if v == nil {
		return nil
	}

	switch v.(type) {
	default:
		return v
	case int:
		if n, ok := v.(int); ok {
			return n
		}
	case int64:
		if n, ok := v.(int64); ok {
			return n
		}

	case float32:
		if n, ok := v.(float32); ok {
			return n
		}

	case float64:
		if n, ok := v.(float64); ok {
			return n
		}

	case bool:
		if b, ok := v.(bool); ok {
			return b
		}
	case string:
		if s, ok := v.(string); ok {
			return s
		}
	case time.Time:
		if s, ok := v.(time.Time); ok {
			return s
		}

	case []byte:
		if bt, ok := v.([]byte); ok {

			return ParseByte(_type, bt)
		}

	}

	return v

}

//Map https://github.com/jmoiron/sqlx/blob/master/sqlx.go#L820

func (s *Stmt) Map(moreTypeMap ...map[string]string) ([]map[string]interface{}, error) {
	//TODO allow interfaced
	if s.table == nil || s.table.Schema == nil {
		return nil, fmt.Errorf(`no table or db installed`)
	}
	if s.err != nil {
		return nil, s.err
	}
	typemap := s.table.GetTypeMap()
	query, err := s.table.FindQuery(s.others...)
	if err != nil {
		return nil, err
	}
	if DEBUG {
		log.Println(query)
	}

	if len(moreTypeMap) > 0 {
		for _, m := range moreTypeMap {
			for _k, _v := range m {
				typemap[_k] = _v
			}
		}
	}

	ret, err := s.table.Schema.Map(s.Db, query, typemap)

	if err != nil {
		return nil, err
	}

	if s.table != nil {
		if s.table.Schema != nil && s.table.Schema.LifeCycle != nil {
			for _, f := range s.table.Schema.LifeCycle.AfterFinds {
				if err := f(ret); err != nil {
					return nil, err
				}

			}
		}

		if s.table.LifeCycle != nil {
			for _, f := range s.table.LifeCycle.AfterFinds {
				if err := f(ret); err != nil {
					return nil, err
				}

			}
		}
	}

	return ret, nil
}

func (self *Table) insert(_updates map[string]interface{}) error {

	query, err := self.InsertQuery(_updates)
	if err != nil {
		return err
	}
	if DEBUG {
		log.Println(query)
	}
	_, err = self.Schema.Db.Exec(query)
	return err
}

func (self *Table) Insert(_updates map[string]interface{}) error {

	if self.Schema != nil && self.Schema.LifeCycle != nil {
		for _, f := range self.Schema.LifeCycle.BeforeCreates {
			if err := f(_updates); err != nil {
				return err
			}

		}
	}

	if self.LifeCycle != nil {
		for _, f := range self.LifeCycle.BeforeCreates {
			if err := f(_updates); err != nil {
				return err
			}

		}
	}

	//	dl, ok := self.Schema.loader.(Dialect)
	//	if ok {
	//		if err := dl.Insert(self, _updates); err != nil {
	//			return err
	//		}
	//	}

	if err := self.insert(_updates); err != nil {
		return err
	}

	if self.Schema != nil && self.Schema.LifeCycle != nil {
		for _, f := range self.Schema.LifeCycle.AfterCreates {
			if err := f(_updates); err != nil {
				return err
			}

		}
	}

	if self.LifeCycle != nil {
		for _, f := range self.LifeCycle.AfterCreates {
			if err := f(_updates); err != nil {
				return fmt.Errorf(`AfterCreates err:%s`, err.Error())
			}

		}
	}

	return nil

}

func (self *Table) update(crit, updates map[string]interface{}) error {
	query, err := self.UpdateQuery(crit, updates)
	if err != nil {
		return err
	}
	if DEBUG {
		log.Println(query)
	}
	_, err = self.Schema.Db.Exec(query)

	if err != nil {
		return fmt.Errorf(`update query err: %s  ;query %s`, query, err.Error())
	}

	return err
}

func (self *Table) Update(crit, updates map[string]interface{}) error {

	if self.Schema != nil && self.Schema.LifeCycle != nil {
		for _, f := range self.Schema.LifeCycle.BeforeUpdates {
			if err := f(crit, updates); err != nil {
				return err
			}

		}
	}

	if self.LifeCycle != nil {
		for _, f := range self.LifeCycle.BeforeUpdates {
			if err := f(crit, updates); err != nil {
				return err
			}

		}
	}

	//	dl, ok := self.Schema.loader.(Dialect)
	//	if ok {
	//		if err := dl.Update(self, crit, updates); err != nil {
	//			return err
	//		}
	//	}

	if err := self.update(crit, updates); err != nil {
		return err
	}

	if self.Schema != nil && self.Schema.LifeCycle != nil {
		for _, f := range self.Schema.LifeCycle.AfterUpdates {
			if err := f(crit, updates); err != nil {
				return err
			}

		}
	}

	if self.LifeCycle != nil {
		for _, f := range self.LifeCycle.AfterUpdates {
			if err := f(crit, updates); err != nil {
				return err
			}

		}
	}

	return nil
}

func (self *Table) remove(crit map[string]interface{}) error {
	query, err := self.RemoveQuery(crit)
	if err != nil {
		return err
	}
	if DEBUG {
		log.Println(query)
	}
	_, err = self.Schema.Db.Exec(query)
	return err
}

func (self *Table) Remove(crit map[string]interface{}) error {

	if self.Schema != nil && self.Schema.LifeCycle != nil {
		for _, f := range self.Schema.LifeCycle.BeforeRemoves {
			if err := f(crit); err != nil {
				return err
			}

		}
	}

	if self.LifeCycle != nil {
		for _, f := range self.LifeCycle.BeforeRemoves {
			if err := f(crit); err != nil {
				return err
			}

		}
	}

	//	dl, ok := self.Schema.loader.(Dialect)
	//	if ok {
	//		if err := dl.Remove(self, crit); err != nil {
	//			return err
	//		}
	//	}

	if err := self.remove(crit); err != nil {
		return err
	}

	if self.Schema != nil && self.Schema.LifeCycle != nil {
		for _, f := range self.Schema.LifeCycle.AfterRemoves {
			if err := f(crit); err != nil {
				return err
			}

		}
	}

	if self.LifeCycle != nil {
		for _, f := range self.LifeCycle.AfterRemoves {
			if err := f(crit); err != nil {
				return err
			}

		}
	}

	return nil
}

func (self *Msi) installForeignKeyMap() {
	if self.ForeignKeyTypeMap == nil {
		self.ForeignKeyTypeMap = make(map[string]string)
	}

	for _, table := range self.Tables {
		for _, field := range table.Fields {

			if field.ReferencedTable == nil { //!!! only install for foreign keys
				continue
			}

			for _, fkField := range field.ReferencedTable.Fields {
				k := fmt.Sprintf("%s__%s", field.Name, fkField.Name)
				self.ForeignKeyTypeMap[k] = fkField.Type
			}

		}
	}
}

func (self *Msi) Map(db *sql.DB, query string, typeMap map[string]string) ([]map[string]interface{}, error) {
	if DEBUG {
		fmt.Println(query)
	}
	if self.ForeignKeyTypeMap == nil {
		self.ForeignKeyTypeMap = make(map[string]string)
	}
	if len(self.ForeignKeyTypeMap) == 0 && len(self.Tables) > 0 {
		self.installForeignKeyMap()
	}
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	ret := []map[string]interface{}{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}
		err = rows.Scan(values...)
		if err != nil {
			return nil, err
		}
		dest := make(map[string]interface{})
		for i, column := range columns {
			dest[column] = *(values[i].(*interface{}))
			_type := `string`

			if __type, ok := typeMap[column]; ok {
				_type = __type
			}

			if __type, ok := self.ForeignKeyTypeMap[column]; ok {
				_type = __type

			}

			dest[column] = ParseVal(_type, dest[column])

		}
		ret = append(ret, dest)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return ret, nil
}

func (t *Table) FindOne(crit ...map[string]interface{}) (M, error) {

	_crit := []map[string]interface{}{}
	if crit == nil {
		_crit = append(_crit, map[string]interface{}{})

	}

	for _, m := range crit {
		_crit = append(_crit, m)
	}

	if len(_crit) == 1 {
		_crit = append(_crit, map[string]interface{}{LIMIT: 1})
	}
	if len(_crit) >= 2 {
		_crit[1][LIMIT] = 1
	}

	founds, err := t.Find(_crit...).Map()
	if err != nil {
		return nil, err
	}
	if len(founds) == 0 {
		return nil, NOT_FOUND
	}
	return founds[0], nil
}
