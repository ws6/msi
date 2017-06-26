package msi

import (
	"database/sql"
)

type DbTable struct {
	Db    *sql.DB
	table *Table
}

type Stmt struct {
	query  string
	Db     *sql.DB
	err    error
	count  int
	others []map[string]interface{}
	table  *Table
}

func (self *DbTable) Find(others ...map[string]interface{}) *Stmt {
	//install configurations
	ret := new(Stmt)
	ret.Db = self.Db
	ret.others = others
	ret.table = self.table
	return ret
}

func (s *Stmt) Count() (int, error) {
	query, err := s.table.Count(s.others...)
	if err != nil {
		return 0, err
	}

	rows, err := s.Db.Query(query)

	if err != nil {
		return 0, err
	}

	var total int
	for rows.Next() {
		err := rows.Scan(&total)
		if err != nil {
			return 0, err
		}
		return total, nil
		break
	}
	return 0, nil
}

//Map https://github.com/jmoiron/sqlx/blob/master/sqlx.go#L820

func (s *Stmt) Map() ([]map[string]interface{}, error) {
	query, err := s.table.Find(s.others...)
	if err != nil {
		return nil, err
	}

	rows, err := s.Db.Query(query)

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
		}
		ret = append(ret, dest)
	}

	return ret, rows.Err()
}

func (self *DbTable) Insert(_updates map[string]interface{}) error {
	query, err := self.table.Insert(_updates)
	if err != nil {
		return err
	}
	_, err = self.Db.Exec(query)
	return err
}

func (self *DbTable) Update(crit, updates map[string]interface{}) error {
	query, err := self.table.Update(crit, updates)
	if err != nil {
		return err
	}
	_, err = self.Db.Exec(query)
	return err
}
func (self *DbTable) UpdateId(id int, updates map[string]interface{}) error {
	return self.Update(map[string]interface{}{`id`: id}, updates)
}

func (self *DbTable) Remove(crit map[string]interface{}) error {
	query, err := self.table.Remove(crit)
	if err != nil {
		return err
	}
	_, err = self.Db.Exec(query)
	return err
}

func (self *DbTable) RemoveId(id int) error {
	return self.Remove(map[string]interface{}{`id`: id})
}
