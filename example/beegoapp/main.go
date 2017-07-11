package main

import (
	_ "github.com/ws6/msi/example/beegoapp/routers"

	"log"

	"github.com/astaxie/beego"
	"github.com/ws6/msi"
)

func main() {
	if beego.BConfig.RunMode == "dev" {
		beego.BConfig.WebConfig.DirectoryIndex = true
		beego.BConfig.WebConfig.StaticDir["/swagger"] = "swagger"
		msi.DEBUG = true
		log.SetFlags(log.LstdFlags | log.Lshortfile) // set log level
	}
	beego.Run()
}
