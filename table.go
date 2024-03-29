package msi

import (
	"fmt"
	"strings"
	"sync"
)

const DEFAULT_LIMIT = 30

type Field struct {
	table    *Table `json:"-"`
	Name     string
	Type     string
	IsNumber bool
	Length   int
	Selected bool
	Hide     bool //not same as selected. selected but hide

	ReferencedTable *Table
	ReferencedField *Field

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

func (self *Field) FullName() string {
	if self.table == nil {
		return self.Name
	}
	//TODO check if it is postgres?
	return fmt.Sprintf("%s.%s", self.table.TableName, self.Name)
}

func (self *Field) GetTableAlias() string {
	return fmt.Sprintf("%s__%s", self.ReferencedTable.TableName, self.Name)
}

func (self *Field) FullNameAS(k, tableAlias string) string { // useing double underscores for uniqueness
	if self.table == nil {
		return self.Name
	}

	return fmt.Sprintf("%s.%s AS %s__%s", tableAlias, self.Name, k, self.Name)
}

type Table struct {
	TableName  string
	*LifeCycle //lifecycle events
	Limit      int
	Schema     *Msi `json:"-"` //pointer back to its parent

	JoinAlias string //used when join queries envoled; for use space
	Fields    []*Field

	unsafeFieldsPatterns   []*pattern
	extraFieldTypeMap      map[string]string
	_extraFieldTypeMapLock sync.Mutex
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

func (t *Table) GetMsi() *Msi {
	return t.Schema
}

func (t *Table) GetFirstPrimaryKey() *Field {
	for _, f := range t.Fields {
		if f.IsPrimaryKey {
			return f
		}
	}

	return nil
}

func (t *Table) SelectAll() {
	t.SelectAllFields()
}

//GetTable for meet querybuilder interface
func (t *Table) GetTable() *Table {
	return t
}

//GetLoader for meet querybuilder interface
func (t *Table) GetLoader() ShemaLoader {
	return t.GetMsi().GetLoader()
}

//SetTypeMap rename from SetExtraTypeMap for querybuilder better naming
func (t *Table) SetTypeMap(k, v string) {
	t.SetExtraTypeMap(k, v)
	return
}
func (t *Table) SetExtraTypeMap(k, v string) {
	t._extraFieldTypeMapLock.Lock()
	defer t._extraFieldTypeMapLock.Unlock()
	if t.extraFieldTypeMap == nil {
		t.extraFieldTypeMap = make(map[string]string)
	}
	if _, ok := t.extraFieldTypeMap[k]; !ok {
		t.extraFieldTypeMap[k] = v
	}

}

func (t *Table) _select(s bool, fields ...string) {
	if t == nil {
		return // for chainning success
	}

	for _, field := range fields {
		if f := t.GetField(field); f != nil {
			f.Selected = s
		}

	}
}

func (t *Table) Select(fields ...string) {
	t._select(true, fields...)
}

func (t *Table) UnSelect(fields ...string) {
	t._select(false, fields...)
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

		ret = append(ret, fmt.Sprintf("%s.%s", t.TableName, k))
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
			_v = fmt.Sprintf("%s", _v)
		}
		if _f.Type == "time.Time" {
			if IsDebug() {
				fmt.Println(_f.Name, _f.Type, _v)

			}

		}

		ret = append(ret, _v)
	}

	return ret
}
func (t *Table) GetTypeMap() map[string]string {

	prim := t.GetPrimTypeMap()

	fkMap := t.GetForeignTableMap()

	for k, v := range fkMap {
		prim[k] = v
	}
	if t.extraFieldTypeMap == nil {
		t.extraFieldTypeMap = make(map[string]string)
	}
	if t.extraFieldTypeMap != nil {
		t._extraFieldTypeMapLock.Lock()
		for k, v := range t.extraFieldTypeMap {
			prim[k] = v
		}
		t._extraFieldTypeMapLock.Unlock()
	}
	return prim
}

func (t *Table) GetPrimTypeMap() map[string]string { //filename->type
	ret := make(map[string]string)
	for _, f := range t.Fields {
		ret[f.Name] = f.Type
	}

	return ret
}

//usergroup__group_id.status

func (t *Table) GetForeignTableMap() map[string]string {
	ret := make(map[string]string)
	for _, f := range t.Fields {
		if f.ReferencedField == nil || f.ReferencedTable == nil {
			continue
		}
		for _, rf := range f.ReferencedTable.Fields {
			k := fmt.Sprintf("%s.%s", f.GetTableAlias(), rf.Name)
			ret[k] = rf.Type
		}
	}
	return ret
}
