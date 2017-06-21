package msi

import (
	"fmt"
	"strings"
	"time"
)

func InterfaceToString(i interface{}) string {

	if i == nil {
		return `null` //!!! mysql dialect
	}
	if s, ok := i.(string); ok {
		return fmt.Sprintf("'%s'", Escape(s))
	}
	if s, ok := i.(bool); ok {
		if s {
			return `true`
		}
		return `false`
	}

	if s, ok := i.(int); ok {
		return fmt.Sprintf(`%d`, s)
	}
	if s, ok := i.(int64); ok {
		return fmt.Sprintf(`%d`, s)
	}

	if s, ok := i.(float32); ok {
		return fmt.Sprintf(`%f`, s)
	}
	if s, ok := i.(float64); ok {
		return fmt.Sprintf(`%f`, s)
	}

	if s, ok := i.(time.Time); ok {
		return fmt.Sprintf("'%s'", Escape(s.String())) //TODO to be better formatted
	}
	return ""
}

func Stringify(updates map[string]interface{}) map[string]string {
	ret := make(map[string]string)

	for k, v := range updates {
		ret[k] = InterfaceToString(v)
	}

	return ret

}

func (t *Table) Insert(_updates map[string]interface{}) string {
	//INSERT INTO person (first_name,last_name,email) VALUES (:first,:last,:email)

	updates := []*NameVal{}
	for k, v := range Stringify(_updates) {
		updates = append(updates, &NameVal{k, v})
	}

	return fmt.Sprintf(
		`INSERT INTO %s ( %s ) VALUES ( %s) ;`,
		t.TableName,
		strings.Join(t.MakeInsertFields(updates), ","),
		strings.Join(t.MakeInsertValues(updates), ","),
	)

}