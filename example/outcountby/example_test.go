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
	sampleTable := db.GetTable(`sample`)
	// libraryTable := db.GetTable(`analysis_sample_id`)

	table := sampleTable
	//tests
	testStr := `http://host.com:5432/path?id=72561&_sortby=analysis_sample_id__sample_id__analysis_id__outcount&id=72707&_populates=project_id&_outcountby=analysis_sample_id__sample_id__id|analysis_sample_id__sample_id__analysis_id`

	u, err := url.Parse(testStr)
	if err != nil {
		t.Fatal(err.Error())
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, f := range table.Fields {
		if f.IsPrimaryKey {
			t.Log(`found primary key`, f)

		}
	}

	t.Log(`OutCountBy`, q.Get(`_outcountby`))
	// table.SetExtraTypeMap(`analysis_sample_id__sample_id__analysis_id__outcount`, `int64`)
	fieldMap := table.GetTypeMap()

	if true {
		body, _ := json.MarshalIndent(fieldMap, "", "  ")
		t.Log(string(body))
	}
	qb, err := querybuilder.Build(q, table)
	if err != nil {
		t.Fatal(err.Error())
	}
	if false {
		body, _ := json.MarshalIndent(qb, "", "  ")
		t.Log(string(body))
	}

	if true {
		t.Logf("qb.OutCountBy====>  %+v\n", qb.OutCountBy)
		t.Logf("qb.Populates====>  %+v\n", qb.Populates)
	}

	crit := qb.Critiera
	if true {
		body, _ := json.MarshalIndent(crit, "", "  ")
		t.Log(`qb.Critiera====>`, string(body))
	}
	others := []map[string]interface{}{crit}

	metaQuery := map[string]interface{}{
		msi.LIMIT:        qb.Limit,
		msi.OFFSET:       qb.Skip,
		msi.ORDERBY:      qb.SortBy,
		msi.GROUPBY:      qb.GroupBy,
		msi.POPULATES:    qb.Populates,
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
