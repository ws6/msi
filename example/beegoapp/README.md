beegoapp is an example made of msi as model layer.

You can expose all CRUD operations  as RESTAPI without worrying about schema change

router/router.go
After init, you can have Number of tables X Five CRUD operations

```
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
	beego.Router("/api/blueprints/:tablename/:id",
		&controllers.BlueprintsController{},
		"post:Create")
```

models/blueprints.go is extremly simple (maybe danger)


controllers/blueprints.go is a transformation from model to routers.
There is a very helpful library -> querybuilder to convert url to msi like syntax.
