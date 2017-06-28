package controllers

//blueprints controller provides CRUD operations from the box
import (
	"fmt"

	"encoding/json"

	"github.com/astaxie/beego"

	"github.com/ws6/msi"
	"github.com/ws6/msi/example/beegoapp/models"
	"github.com/ws6/msi/querybuilder"
)

type BlueprintsController struct {
	beego.Controller
}

func GetTableFromContext(c *BlueprintsController) (*msi.Table, error) {
	db := models.GetSchema()
	tablename := c.Ctx.Input.Param(":tablename")
	table := db.GetTable(tablename)
	if table == nil {
		return nil, fmt.Errorf(`not table found %s`, tablename)
	}
	return table, nil

}

func MakeFindFunc(c *BlueprintsController, others ...map[string]interface{}) func(*BlueprintsController) {
	return func(c *BlueprintsController) {
		table, err := GetTableFromContext(c)

		if err != nil {
			badRequest(c.Ctx, err.Error())
			return
		}

		if len(others) == 0 {
			typeMap := table.GetTypeMap()
			//querybuilder.Build can make a url query into msi interface
			// ?field=abc&field1=$in:1,2,3&limit=100&offset=25
			qb, err := querybuilder.Build(c.Input(), typeMap)
			if err != nil {
				badRequest(c.Ctx, "querybuilder.Build Err: "+err.Error())
				return
			}

			crit := qb.Critiera
			others = append(others, crit)
			metaQuery := map[string]interface{}{
				`$limit`:   qb.Limit,
				`$offset`:  qb.Skip,
				`$orderby`: qb.SortBy,
			}
			others = append(others, metaQuery)
		}

		founds, err := table.Find(others...).Map()
		if err != nil {
			serverError(c.Ctx, err.Error())
			return
		}

		c.Data["json"] = founds
		c.ServeJSON()
	}
}

func (c *BlueprintsController) GetOne() {
	//suppose each table have a primary key "id"
	crit := map[string]interface{}{`id`: c.Ctx.Input.Param(":id")}
	f := MakeFindFunc(c, crit)
	f(c)
}

func (c *BlueprintsController) GetAll() {
	f := MakeFindFunc(c)
	f(c)
}

func (c *BlueprintsController) Update() {

	table, err := GetTableFromContext(c)
	if err != nil {
		badRequest(c.Ctx, err.Error())
		return
	}

	//How simple to use generic map[string]interface{} and without worry about empty type v.s. null
	updates := make(map[string]interface{})
	if err := json.Unmarshal(c.Ctx.Input.RequestBody, updates); err != nil {
		badRequest(c.Ctx, err.Error())
		return
	}

	if err := table.Update(map[string]interface{}{`id`: c.Ctx.Input.Param(":id")}, updates); err != nil {
		badRequest(c.Ctx, fmt.Sprintf("update err: %s", err.Error()))
		return
	}

	Okay(c.Ctx)
}

//RemoveId !!!danger to expose it this way. better wrap with your own access control logics before let user seeing it
func (c *BlueprintsController) RemoveId() {

	table, err := GetTableFromContext(c)
	if err != nil {
		badRequest(c.Ctx, err.Error())
		return
	}

	if err := table.Remove(map[string]interface{}{`id`: c.Ctx.Input.Param(":id")}); err != nil {
		badRequest(c.Ctx, fmt.Sprintf("update err: %s", err.Error()))
		return
	}

	Okay(c.Ctx)
}

//TODO adding lifecycle functions
func (c *BlueprintsController) Create() {

	table, err := GetTableFromContext(c)
	if err != nil {
		badRequest(c.Ctx, err.Error())
		return
	}

	//How simple
	toCreate := make(map[string]interface{})
	if err := json.Unmarshal(c.Ctx.Input.RequestBody, toCreate); err != nil {
		badRequest(c.Ctx, err.Error())
		return
	}

	if err := table.Insert(toCreate); err != nil {
		badRequest(c.Ctx, fmt.Sprintf("insert err: %s", err.Error()))
		return
	}

	Okay(c.Ctx)
}
