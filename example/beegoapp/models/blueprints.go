package models

import (
	"github.com/astaxie/beego"
	"github.com/ws6/msi"

	_ "github.com/go-sql-driver/mysql"
)

var (
	_schema *msi.Msi
)

func getConfigString() string {
	//currently only support mysql
	//make sure your have below line from your conf/app.conf file
	//mysql_connetion = username:password@(hostname:3306)/databasename
	return beego.AppConfig.String(`mysql_connetion`)
}

func init() {
	schema, err := msi.NewDb(`mysql`, getConfigString(), `sage`, ``)
	if err != nil {
		panic(err.Error())
	}
	_schema = schema
}

//simplest models layer code
func GetSchema() *msi.Msi {
	return _schema
}
