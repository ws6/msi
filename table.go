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
	DbName    string
	JoinAlias string //used when join queries envoled; for use space
	Fields    []*Field
}

func IsNumber(t string) bool {

	if strings.Contains(t, `int`) {
		return true
	}
	if strings.Contains(t, `float`) {
		return true
	}
	return false

}

func (t *Table) SelectAll() {
	t.SelectAllFields()
}

func (t *Table) SelectAllFields() {
	for _, f := range t.Fields {
		f.Selected = true
	}
}

func (t *Table) UnSelectAllFields() {
	for _, f := range t.Fields {
		f.Selected = false
	}
}

//fun :)
func (t *Table) ToggleSelectAllFields() {
	for _, f := range t.Fields {
		f.Selected = !f.Selected
	}
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
		_v := v

		if !_f.IsNumber {
			_v = fmt.Sprintf("'%s'", _v)
		}

		ret = append(ret, _v)
	}

	return ret
}
