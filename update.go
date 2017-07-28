package msi

import (
	"fmt"
	"strings"
)

func (t *Table) SafeUpdate(updates map[string]interface{}) []string {
	up := []string{}
	for k, v := range Stringify(updates) {
		field := t.GetField(k)
		if field == nil {
			continue
		}
		if field.Name == `id` {
			continue //!!! biased design
		}

		_v := v

		if !field.IsNumber {
			_v = fmt.Sprintf("%s", _v)
		}

		up = append(up, fmt.Sprintf("%s.%s=%s", t.TableName, k, _v))
	}
	return up
}

func (t *Table) UpdateQuery(crit, updates map[string]interface{}) (string, error) {
	up := t.SafeUpdate(updates)
	ret := fmt.Sprintf(`UPDATE %s SET %s`,
		t.TableName,
		strings.Join(up, ", "),
	)
	if crit == nil {
		return fmt.Sprintf("%s ;", ret), nil
	}
	whereClause, err := t.SafeWhere(crit)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`%s %s;`, ret, whereClause), nil
}
