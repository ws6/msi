package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/ws6/msi"

	_ "github.com/go-sql-driver/mysql"
)

func GetSchema() *msi.Msi {
	schema, err := msi.NewDb(`mysql`, `username:password@(hostname:3306)/databasename `, `databasename`, ``)
	if err != nil {
		panic(err.Error())
	}
	return schema
}

var db *msi.Msi

func init() {
	db = GetSchema()
}

func handler(w http.ResponseWriter, r *http.Request) {

	//expect to see /{tablename}/{idStr [optional]} pattern
	urlSplit := strings.Split(r.URL.Path, "/")
	tablename := ""
	idStr := ""
	if len(urlSplit) > 1 {
		tablename = urlSplit[1]
	}

	table := db.GetTable(tablename)
	if table == nil {
		fmt.Fprintf(w, `not table found`)
		return
	}

	if len(urlSplit) > 2 {
		idStr = urlSplit[2]
	}

	if r.Method == http.MethodGet {
		var founds interface{}
		var err error
		if idStr == "" {
			founds, err = table.GetPage(nil, nil) //TODO parse URL into query map
		}
		if idStr != "" {
			founds, err = table.Find(msi.M{`id`: idStr}).Map() //TODO parse URL into query map
		}
		if err != nil {
			fmt.Fprintf(w, err.Error())
			return
		}
		bt, err := json.Marshal(founds)
		if err != nil {
			fmt.Fprintf(w, err.Error())
			return
		}

		w.Header().Set(`Content-Type`, `application/json`)
		fmt.Fprintf(w, string(bt))
		return
	}

	if r.Method == http.MethodPut || r.Method == http.MethodPost { //update
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Fprintf(w, err.Error())
			return
		}
		updates := make(map[string]interface{})
		if err := json.Unmarshal(body, &updates); err != nil {
			fmt.Fprintf(w, err.Error())
			return
		}
		if r.Method == http.MethodPut {
			if err := table.Update(msi.M{`id`: idStr}, updates); err != nil {
				fmt.Fprintf(w, err.Error())
				return
			}
			fmt.Fprintf(w, `Updated`)
			return
		}
		if r.Method == http.MethodPost {
			if err := table.Insert(updates); err != nil {
				fmt.Fprintf(w, err.Error())
				return
			}
			fmt.Fprintf(w, `Created`)
			return
		}
	}

}
func main() {
	http.HandleFunc("/", handler)
	http.ListenAndServe(":8080", nil)
}
