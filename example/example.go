package examples

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ws6/msi"
)

func TestLoadTables(t *testing.T) {

	schema, err := msi.NewMsi(`mysql`, `username:password@(localhost:3306)/databasename`, `databasename`, ``)
	if err != nil {
		t.Fatal()
	}

	flowcell := schema.GetTable(`flowcell`)
	if flowcell == nil {
		t.Fatal(`no flowcell table found`)
	}
	query, err := flowcell.Find(nil)

	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(query)

	crit := map[string]interface{}{
		`not_exist_field`:  `should get filtered`,
		`id`:               1234,
		`flowcell_barcode`: map[string]interface{}{`$like`: `%ABBA%`},
		`RecipePath`:       map[string]interface{}{`$in`: []string{`/illumina`, `/scratch`, `\\ussd-prd-isi04\Voyager`}},
	}
	query2, err := flowcell.Find(crit)

	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(query2)
	if barcodeField := flowcell.GetField(`flowcell_barcode`); barcodeField != nil {
		barcodeField.Selected = true
	}
	if barcodeField := flowcell.GetField(`RecipePath`); barcodeField != nil {
		barcodeField.Selected = true
	}
	query3, err := flowcell.Find(crit)

	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(query3)

	query4, err := flowcell.FindId(19810505)

	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(query4)

}
