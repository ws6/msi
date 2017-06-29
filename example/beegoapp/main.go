package main

import (
	_ "github.com/ws6/msi/example/beegoapp/routers"

	"github.com/astaxie/beego"
	"github.com/ws6/msi"
)

func main() {
	if beego.BConfig.RunMode == "dev" {
		beego.BConfig.WebConfig.DirectoryIndex = true
		beego.BConfig.WebConfig.StaticDir["/swagger"] = "swagger"
		msi.DEBUG = true
	}
	beego.Run()
}
