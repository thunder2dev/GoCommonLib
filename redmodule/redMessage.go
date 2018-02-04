package redmodule

import (
	"MarsXserver/common"
)

type ObjMessageId int

const (
	_  ObjMessageId = common.MessageId_RedMessageIdStart + iota
	MessageId_Data_Op_Req
	MessageId_Data_Op_Rsp
)

type RedDataOpType int
const (
	_	RedDataOpType = iota
	RedDataOpRead
	RedDataOpSave
	RedDataOpUpdate
	RedDataOpDel
	RedDataOpCnt
	RedDataOpMax
)



type RedDataRequest struct{
	Op         RedDataOpType
	StructName string
	IdVals	   []int64
	StructNames []string
	IdValsArr	[][]int64
	//FieldIdxs  []int64
	FieldVals  []string
	CntIdx	   int
}


type RedDataResponse struct{
	Op         RedDataOpType
	Data interface{}
}
















