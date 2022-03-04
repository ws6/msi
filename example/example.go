package examples

import (
	"testing"

	"log"

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

	debug := msi.IsDebug()
	msi.IsDebug() = true
	oldFlags := log.Flags()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	defer func() {
		msi.IsDebug() = debug
		log.SetFlags(oldFlags)
	}()
	schema, err := msi.NewDb(`mysql`, `username:password@(localhost:3306)/databasename`, `databasename`, ``)

	if err != nil {
		t.Fatal()
	}

	defer schema.Close()

	for _, table := range schema.Tables {

		page, err := table.GetPage(nil, msi.M{msi.LIMIT: 10, msi.OFFSET: 5})
		if err != nil {
			t.Fatal(table.TableName, err.Error())
			continue
		}

		t.Logf("%+v  ", page)

	}

	return
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

	query, err := flowcell.FindQuery(where, meta)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log(query)

}
