package msi

import (
	"fmt"
)

var ERR_NOT_IMPL = fmt.Errorf(`not implemented`)

func (self *MSSQLLoader) Insert(*Table, map[string]interface{}) error {
	return ERR_NOT_IMPL
}
func (self *MSSQLLoader) Update(t *Table, crit, updates map[string]interface{}) error {
	return ERR_NOT_IMPL
}
func (self *MSSQLLoader) Remove(t *Table, crit map[string]interface{}) error {
	return ERR_NOT_IMPL
}

func (self *MSSQLLoader) FindQuery(t *Table, crit ...map[string]interface{}) (string, error) {
	return "", ERR_NOT_IMPL
}

func (self *MSSQLLoader) CountQuery(t *Table, others ...map[string]interface{}) (string, error) {
	return "", ERR_NOT_IMPL
}
