package msi

import (
	"fmt"
	"strings"
)

type Field struct {
	Name     string
	Type     string
	IsNumber bool
	Length   int
	Selected bool
	//ported from https://github.com/mijia/modelq, not used in msi
	JsonMeta        string
	IsNullable      bool
	IsPrimaryKey    bool
	IsUniqueKey     bool
	IsIndexed       bool
	IsAutoIncrement bool
	DefaultValue    string
	Extra           string
	Comment         string
}

type Table struct {
	TableName string
	Fields    []*Field
}

var (
	TABLES = make(map[string]*Table)
)

func IsNumber(t string) bool {

	if strings.Contains(t, `int`) {
		return true
	}
	if strings.Contains(t, `float`) {
		return true
	}
	return false

}

func Register(t *Table) {

	for _, f := range t.Fields {
		if IsNumber(f.Type) {
			f.IsNumber = true
		}
	}

	TABLES[t.TableName] = t
}

func GetTable(tableName string) *Table {
	if t, ok := TABLES[tableName]; ok {
		return t
	}

	return nil
}

func (t *Table) GetFieldNames() []string {
	ret := []string{}
	for _, f := range t.Fields {
		ret = append(ret, f.Name)
	}
	return ret
}

func (t *Table) GetField(f string) *Field {
	for _, field := range t.Fields {
		if field.Name == f {
			return field
		}
	}
	return nil
}

type NameVal struct {
	Name  string
	Value string
}

func (t *Table) MakeInsertFields(updates []*NameVal) []string {
	ret := []string{}

	for _, item := range updates {
		k := item.Name
		if _f := t.GetField(k); _f == nil {
			continue
		}

		ret = append(ret, k)
	}

	return ret
}

func (t *Table) MakeInsertValues(updates []*NameVal) []string {
	ret := []string{}

	for _, item := range updates {
		k, v := item.Name, item.Value
		_f := t.GetField(k)
		if _f == nil {
			continue
		}
		_v := Escape(v)

		if !_f.IsNumber {
			_v = fmt.Sprintf("'%s'", _v)
		}

		ret = append(ret, _v)
	}

	return ret
}
