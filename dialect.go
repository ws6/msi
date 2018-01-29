package msi

import (
	"fmt"
)

var dialector = map[string]Dialect{}

var ERR_USE_MYSQL = fmt.Errorf(`use_mysql_default_implementation`)

type Dialect interface {
	//	Insert(*Table, map[string]interface{}) error
	//	Update(t *Table, crit, updates map[string]interface{}) error
	//	Remove(t *Table, crit map[string]interface{}) error

	InsertQuery(t *Table, _updates map[string]interface{}) (string, error)
	UpdateQuery(t *Table, crit, updates map[string]interface{}) (string, error)
	RemoveQuery(t *Table, crit map[string]interface{}) (string, error)

	FindQuery(t *Table, crit ...map[string]interface{}) (string, error)
	CountQuery(t *Table, others ...map[string]interface{}) (string, error)
	//	FindOne(t *Table, crit ...map[string]interface{}) (M, error)
	//	Find(t *Table, others ...map[string]interface{}) *Stmt
	//	Count(s *Stmt) (int, error)
	//	GetPage(t *Table, others ...map[string]interface{}) (*Page, error)
	GetGroupCountPage(t *Table, others ...map[string]interface{}) (*Page, error)
}
