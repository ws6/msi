package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
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

func TestAllMsSqlOutCountBy(t *testing.T) {
	db, err := GetTestMSI()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()

	testStr := `http://host.com:5432/path?_outcountby=analysis_sample_id__sample_id__id&_sortby=t2__user_created__is_ro desc&t0__project_id__name=covidseqtest&_populates=user_created&_populates2=project_id->customer_id->user_created:t0__project_id__name,t1__customer_id__name|user_created:t0__user_created__name`
	testStr2 := `http://host.com:5432/path?_limit=30&t1__asa_id__case_instance_id=67&_limit=30&_skip=0&_populates2=%s&_sortby=id desc`

	// ||cat:_fastq_lane_data_set.library_id->asa_library.library_id:cat__lane_number
	recursivePopulateAsaSample := `asa_sample_id->asa_id->workflow_id`
	recursivePopulateLibraryId := `library_id->analysis_id`
	//specifyModelPop format
	//$alias:$leftTable.$leftKey->$rightTable.$rightKey:$fields,,,

	specifyModelPop := `cat:_fastq_lane_data_set.library_id->asa_library.library_id:cat__lane_number`
	//another example shows the right model can be any existing join alias. it not needs to be a tableName msi knows.
	specifyModelPop2 := `dog:analysis_qc_metric.analysis_sample_id->t0__library_id.id:`

	pops := []string{
		recursivePopulateAsaSample,
		recursivePopulateLibraryId,
		specifyModelPop,
		specifyModelPop2,
	}
	popStr := strings.Join(pops, `|`)
	testStr2 = fmt.Sprintf(testStr2, popStr)

	tests := [][2]string{
		{`sample`, testStr},
		{`asa_library`, testStr2},
	}

	for _, test := range tests {
		t.Log(test)
		DoTestMsSqlOutCountBy(t, db, test)
	}
}

func DoTestMsSqlOutCountBy(t *testing.T, db *msi.Msi, test [2]string) {

	tableName := test[0]
	table := db.GetTable(tableName)

	testStr := test[1]

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
		t.Logf("qb.Populates2====>  %+v\n", qb.Populates2)
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
		msi.LIMIT:        qb.Limit,
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

	_founds, err := table.GetPage(others...)
	if err != nil {
		t.Fatal(err.Error())
	}
	if true {
		body, _ := json.MarshalIndent(_founds, "", "  ")
		t.Log(string(body))
	}
}
