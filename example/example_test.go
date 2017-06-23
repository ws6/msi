package examples

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ws6/msi"
)

var (
	flowcell = &msi.Table{
		TableName: "flowcell",
		Fields: []*msi.Field{
			{Name: "id", Type: "int"},
			{Name: "flowcell_resource_id", Type: "int"},
			{Name: "flowcell_barcode", Type: "string"},
			{Name: "RecipePath", Type: "string"}, // firefly only
		},
	}
)

func TestLoadTables(t *testing.T) {

	schema, err := msi.NewMsi(`mysql`, `username:password@(localhost:3306)/databasename`, `databasename`, ``)
	if err != nil {
		t.Fatal()
	}
	t.Log(`Total Number of Tables: `, len(schema.Tables))
	t.Log(`##############`)
	crit := map[string]interface{}{`id`: 123}
	updates := map[string]interface{}{`id`: `updated values`}
	for _, table := range schema.Tables {
		t.Log(`###`)
		t.Log(` Table:`, table.TableName, `,`, `number of fields:`, len(table.Fields))
		table.SelectAll()
		query, err := table.FindId(123)
		if err != nil {
			t.Fatal(err.Error())
		}

		table.UnSelectAllFields()
		t.Log(query)
		query, err = table.Find(crit)
		if err != nil {
			t.Fatal(err.Error())
		}

		t.Log(query)
		query, err = table.UpdateId(1, updates)
		if err != nil {
			t.Fatal(err.Error())
		}
		t.Log(query)
		query, err = table.RemoveId(123)
		if err != nil {
			t.Fatal(err.Error())
		}
		t.Log(query)
		query, err = table.Insert(updates)
		if err != nil {
			t.Fatal(err.Error())
		}
		t.Log(query)

	}
}

func TestParseGroupBY(t *testing.T) {
	where := map[string]interface{}{
		`id`: map[string]interface{}{`$in`: []int{123, 456, 789}},
	}
	meta := map[string]interface{}{
		msi.LIMIT: 5, msi.OFFSET: 101,
		msi.GROUPBY: []string{`flowcell_barcode`, `RecipePath`},
		msi.ORDERBY: []string{`flowcell_barcode desc`},
		msi.FIELDS:  []string{`flowcell_barcode`},
	}

	query, err := flowcell.Find(where, meta)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log(query)

}
