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
			{Name: "status", Type: "string"},
			{Name: "server", Type: "string"},
			{Name: "flowcell_barcode", Type: "string"},
			{Name: "run_id", Type: "string"},
			{Name: "run_number", Type: "string"},
			{Name: "indexed", Type: "string"},
			{Name: "read_length", Type: "string"},
			{Name: "machine_name", Type: "string"},
			{Name: "update_timestamp", Type: "time.Time"},
			{Name: "application_name", Type: "string"},
			{Name: "application_version", Type: "string"},
			{Name: "fpga_version", Type: "string"},
			{Name: "rta_version", Type: "string"},
			{Name: "run_param_output_folder", Type: "string"},
			{Name: "description", Type: "string"},
			{Name: "location", Type: "string"},
			{Name: "run_start_date", Type: "time.Time"},
			{Name: "instrument_type", Type: "string"},
			{Name: "chemistry", Type: "string"},
			{Name: "folder_exists", Type: "string"},
			{Name: "keep_data", Type: "string"},
			{Name: "cycles", Type: "int"},
			{Name: "current_cycle", Type: "int"},
			{Name: "cif_first", Type: "time.Time"},
			{Name: "cif_latest", Type: "time.Time"},
			{Name: "raptor_port", Type: "int"},
			{Name: "raptor_fc_id", Type: "int"},
			{Name: "updatedAt", Type: "time.Time"},
			{Name: "createdAt", Type: "time.Time"},
			{Name: "surface_status", Type: "string"},
			{Name: "subtile_status", Type: "string"},
			{Name: "temp_status", Type: "string"}, // tile metrics status
			{Name: "planr_id", Type: "int"},
			{Name: "sav_version", Type: "string"},
			{Name: "sav_failed_reason", Type: "string"},
			{Name: "percent_pf", Type: "float64"},
			{Name: "total_pf_yields_gb", Type: "float64"},
			{Name: "mean_error_rate_r1", Type: "float64"},
			{Name: "mean_error_rate_r2", Type: "float64"},
			{Name: "mean_percent_q30", Type: "float64"},
			{Name: "sav_status", Type: "int"}, // number of cycles extracted by sav
			{Name: "mean_pct_aligned", Type: "float64"},
			{Name: "RfidsInfoStatus", Type: "string"}, // null is not done any value will stop parsing again
			{Name: "FlowCellSerialBarcode", Type: "string"},
			{Name: "LibraryTubeSerialBarcode", Type: "string"},
			{Name: "SbsSerialBarcode", Type: "string"},
			{Name: "BufferSerialBarcode", Type: "string"},
			{Name: "SbsLotNumber", Type: "string"},
			{Name: "BufferLotNumber", Type: "string"},
			{Name: "ClusterLotNumber", Type: "string"},
			{Name: "ClusterSerialBarcode", Type: "string"},
			{Name: "ClusterPartNumber", Type: "string"},
			{Name: "ClusterExpirationdate", Type: "time.Time"},
			{Name: "ClusterCycleKit", Type: "string"},
			{Name: "ClusterRssi", Type: "string"},
			{Name: "reagent_status", Type: "string"},
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
