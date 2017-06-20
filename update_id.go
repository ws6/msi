package msi

import (
	"fmt"
	"strings"
)

func (t *Table) UpdateId(id int, updates map[string]interface{}) string {

	up := []string{}
	for k, v := range Stringify(updates) {
		field := t.GetField(k)
		if field == nil {
			continue
		}
		if field.Name == `id` {
			continue //!!! biased design
		}

		_v := Escape(v)

		if !field.IsNumber {
			_v = fmt.Sprintf("'%s'", _v)
		}

		up = append(up, fmt.Sprintf("%s=%s", k, _v))
	}

	return fmt.Sprintf(`
		UPDATE %s SET
		%s
		WHERE id = %d 
		;
	`,
		t.TableName,
		strings.Join(up, ", "),
		id,
	)

}
