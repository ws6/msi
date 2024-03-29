package msi

import (
	"fmt"
)

func (t *Table) RemoveQuery(crit map[string]interface{}) (string, error) {
	if crit == nil {
		return "", fmt.Errorf(`can not remove without where clause`)
	}

	if dl, ok := t.Schema.GetLoader().(Dialect); ok {
		return dl.RemoveQuery(t, crit)
	}
	whereClause, err := t.SafeWhere(crit)
	if err != nil {
		return "", err
	}
	ret := fmt.Sprintf(`DELETE FROM %s %s `, t.TableName, whereClause)
	return ret, nil
}
