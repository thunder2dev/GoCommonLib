package orm

import (
	"MarsXserver/common"
)

type ObjMessageId int

const (
	_  ObjMessageId = common.MessageId_DbObjMessageIdStart + iota
	MessageId_DB_Expr_Req
	MessageId_DB_Expr_Rsp

)


type DBExprRequest struct{
	Expr *XOrmEprData
}


type DBExprResponse struct{
	Data []interface{}

}