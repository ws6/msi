package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/ws6/msi"
	"github.com/ws6/msi/querybuilder"
)

func MakeConnectionString(host, db, user, pass string) string {

	return fmt.Sprintf("server=%s;database=%s;user id=%s;password=%s",
		host,
		db,
		user,
		pass,
	)
}

func GetTestConnectionString() string {
	return MakeConnectionString(
		os.Getenv(`MSI_TEST_HOST`),
		os.Getenv(`MSI_TEST_DB`),
		os.Getenv(`MSI_TEST_USER`),
		os.Getenv(`MSI_TEST_PASS`),
	)
}
func GetTestMSI() (*msi.Msi, error) {
	return msi.NewDb(msi.MSSQL, GetTestConnectionString(), os.Getenv(`MSI_TEST_DB`), "")

}

func TestMsSqlOutCountBy(t *testing.T) {

	db, err := GetTestMSI()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	table := db.GetTable(`analysis`)

	//tests
	// testStr := `http://localhost:4200/api/blueprints/analysis?_groupcountby=analysis.status&_populates=env_id|workflow_id&_sortby=analysis.status&status=launched`
	// testStr := `https://matcha-verification.rancher-prd.illumina.com/api/blueprints/analysis?_groupcountby=analysis.status&_populates=env_id|workflow_id&_sortby=status&status=intra.finished`
	// testStr := `http://localhost:4200/api/blueprints/analysis?_groupcountby=analysis.report_type&_populates=env_id%7Cworkflow_id&_sortby=report_type&status=Delivered`

	tests := []string{
		`http://localhost:4200/api/blueprints/analysis?_groupcountby=analysis.status&_populates=env_id|workflow_id&_sortby=status&status=intra.finished`,
		`http://localhost:4200/api/blueprints/analysis?_groupcountby=analysis.report_type&_populates=env_id|workflow_id&_sortby=report_type&status=failed`,
		`http://localhost:4200/api/blueprints/analysis?_groupcountby=analysis.status&_populates=env_id|workflow_id&_sortby=analysis.status&status=launched`,
	}
	for _, testStr := range tests {

		u, err := url.Parse(testStr)
		if err != nil {
			t.Fatal(err.Error())
		}
		q, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			t.Fatal(err.Error())
		}
		// table.GetTypeMap()
		qb, err := querybuilder.Build(q, table)
		if err != nil {
			t.Fatal(err.Error())
		}

		if true {
			t.Logf("qb.GroupCountBy====>  %+v\n", qb.GroupCountBy)
			t.Logf("qb.Critiera====>  %+v\n", qb.Critiera)
		}

		ms := new(msi.MSSQLLoader)

		// nextTable *Table, //the fk table key is using
		// newTempTableName string,
		// newPKName string,
		// selectedFields []string,
		// nonSelectClause string,
		// err error,
		if false {
			_, newTempTableName, newPKName, _, nonSelectClause, _, err := ms.CompilePopulates2(
				table,        //current table
				`project_id`, //key
				1,            //order
				"", "",       //pre table, pre pk
				nil, //allow fields
			)
			if err != nil {
				t.Fatal(err.Error())
			}
			t.Log(newTempTableName)
			t.Log(newPKName)
			t.Log(nonSelectClause)
		}

		if false {
			mq := new(msi.MetaQuery)
			mq.Populates2 = qb.Populates2
			_, joins, _, err := ms.CompileAllPopulates2(table, mq.Populates2)
			if err != nil {
				t.Fatal(err.Error())
			}

			for _, join := range joins {
				t.Log(join)
			}

			return
		}
		crit := qb.Critiera
		db.GetTable(`user`).GetField(`session_id`).Hide = true
		db.GetTable(`user`).GetField(`password_md5`).Hide = true
		t.Log(`password_md5 type==>`, db.GetTable(`user`).GetField(`password_md5`).Type)
		others := []map[string]interface{}{crit}

		metaQuery := map[string]interface{}{
			msi.LIMIT:        1, // qb.Limit,
			msi.OFFSET:       qb.Skip,
			msi.ORDERBY:      qb.SortBy,
			msi.GROUPBY:      qb.GroupBy,
			msi.POPULATES:    qb.Populates,
			msi.POPULATES2:   qb.Populates2,
			msi.GROUPCOUNTBY: qb.GroupCountBy,
			msi.SINCECOUNTBY: qb.SinceCountBy,
			msi.OUTCOUNTBY:   qb.OutCountBy,
		}
		others = append(others, metaQuery)

		_founds, err := table.GetGroupCountPage(others...)
		if err != nil {
			t.Fatal(err.Error())
		}
		if true {
			body, _ := json.MarshalIndent(_founds, "", "  ")
			t.Log(string(body))
		}
	}
}
