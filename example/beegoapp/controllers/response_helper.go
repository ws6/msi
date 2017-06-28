package controllers

import (
	"github.com/astaxie/beego/context"
)

//short hands for dealing with response
type RespJson struct {
	Code   string
	Reason string
}

func Okay(ctx *context.Context) {
	ctx.Output.SetStatus(200)
	ctx.Output.JSON(RespJson{"Okay", "Okay"}, true, true)
}

func badRequest(ctx *context.Context, reason string) {
	ctx.Output.SetStatus(400)
	ctx.Output.JSON(RespJson{"badRequest", reason}, true, true)
}

func serverError(ctx *context.Context, reason string) {
	ctx.Output.SetStatus(500)
	ctx.Output.JSON(RespJson{"serverError", reason}, true, true)
}
