// @APIVersion 1.0.0
// @Title beego Test API
// @Description beego has a very cool tools to autogenerate documents for your API
// @Contact astaxie@gmail.com
// @TermsOfServiceUrl http://beego.me/
// @License Apache 2.0
// @LicenseUrl http://www.apache.org/licenses/LICENSE-2.0.html
package routers

import (
	"github.com/ws6/msi/example/beegoapp/controllers"

	"github.com/astaxie/beego"
)

func init() {
	beego.Router("/api/blueprints/:tablename/:id",
		&controllers.BlueprintsController{},
		"get:GetOne")
	beego.Router("/api/blueprints/:tablename",
		&controllers.BlueprintsController{},
		"get:GetAll")

	beego.Router("/api/blueprints/:tablename/:id",
		&controllers.BlueprintsController{},
		"put:Update")
	beego.Router("/api/blueprints/:tablename/:id",
		&controllers.BlueprintsController{},
		"delete:RemoveId")
	beego.Router("/api/blueprints/:tablename",
		&controllers.BlueprintsController{},
		"post:Create")

}
