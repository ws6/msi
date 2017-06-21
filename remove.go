package msi

import (
	"fmt"
)

func (t *Table) RemoveId(id int) (string, error) {
	return t.Remove(map[string]interface{}{`id`: id})
}

func (t *Table) Remove(crit map[string]interface{}) (string, error) {
	if crit == nil {
		return "", fmt.Errorf(`can not remove without where clause`)
	}
	whereClause, err := t.SafeWhere(crit)
	if err != nil {
		return "", err
	}
	ret := fmt.Sprintf(`DELETE FROM %s %s `, t.TableName, whereClause)
	return ret, nil
}
