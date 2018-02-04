package httpserver

import (
	"MarsXserver/common"
)

func  Auth(handler HttpHandler, ctx *HttpContext)(bool, error){

	defer func(){

		if err := recover(); err != nil{
			common.ErrorLog("auth func panic", err)
		}
	}()

	uidInf := handler.GetSessinon(common.SessionKey_UID)

	if uidInf == nil {
		return false, nil
	}

	common.InfoLog("uid from session", uidInf)


	uid, ok := uidInf.(int64)   //important uid 64

	if !ok || uid <= 0{
		return false, nil
	}

	return true, nil
}



















