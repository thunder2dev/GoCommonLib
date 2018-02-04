package tcpserver

import (
	"reflect"
	"fmt"
	"errors"
	"MarsXserver/common"
)

type MessageProtoType int


const(
	_ MessageProtoType = iota
	MessageProtoType_Proto
	MessageProtoType_ObjRequest
	MessageProtoType_ObjResponse
	MessageProtoType_FileRequest
	MessageProtoType_FileResponse
)



func init(){
	RegisterHandler(
		int(common.MessageId_ObjRequestMessageId),
		int(common.MessageId_ObjReponseMessageId),
		&HandlerItem{
			MsgProtoType: MessageProtoType_ObjRequest,
		},
		&HandlerItem{
			MsgProtoType: MessageProtoType_ObjResponse,
		})
}


type MsgHandler interface {
	Handle(connector *Connector, header interface{}, body interface{}) error
}

type MsgGetIdFunc func(msg interface{}) (msgId int, err error)

type MsgGetSeqFunc func(msg interface{}) (seq int64, err error)

type MsgSetSeqFunc func(msg interface{}, seq int64) error

type MsgSecondDecoder func(buf *common.MBuffer) (interface{}, error)

type HandlerItem struct{

	MsgId int

	MsgProtoType MessageProtoType

	MsgType reflect.Type

	Handler MsgHandler

	PairReqMsgId int
	PairRspMsgType reflect.Type

	IsRequest	bool

	//used by root msg
	//MsgGetIdFunc MsgGetIdFunc

	//MsgGetSeqFunc MsgGetSeqFunc

	//MsgSetSeqFunc MsgSetSeqFunc
	//used by

	IsByPassSecondRspMsgDecoder bool

}

var handlerMap = make(map[int]*HandlerItem)
var handlerReverseMap = make(map[reflect.Type]int)


func getMsgRegItemById(msgid int) (*HandlerItem, error){

	msgItem, check := handlerMap[msgid]
	if !check{
		err := errors.New(fmt.Sprintf("no handler : %v", msgid))
		return nil, err
	}
	return msgItem, nil

}


func getMsgIdByMsg(msg interface{}) (int, error){

	obj := reflect.ValueOf(msg)
	if obj.Kind() != reflect.Ptr{
		return -1, common.ErrorLog("send msg is not pointer struct or nil")
	}

	ind := reflect.Indirect(obj)

	msgId, ok := handlerReverseMap[ind.Type()]
	if ok == false{
		return -1, common.ErrorLog("no such type", ind.Type().Name())
	}

	return msgId, nil
}


func RegisterRequestHandler(reqMsgId int, rspMsgId int, reqHandlerItem* HandlerItem){

	if _, found := handlerMap[reqMsgId]; found == true{

		common.FatalLog(fmt.Sprintf("proto msg id is registered:%v", reqMsgId))
		return
	}

	reqHandlerItem.MsgId = reqMsgId
	reqHandlerItem.IsRequest = true
	handlerMap[reqMsgId] = reqHandlerItem

	rspHandlerItem, found := handlerMap[rspMsgId]

	if found{

		regMutualInfo(reqHandlerItem, rspHandlerItem)
	}

	if reqHandlerItem.MsgType != nil{
		handlerReverseMap[reqHandlerItem.MsgType] = reqMsgId
	}

}


func RegisterResponseHandler(reqMsgId int, rspMsgId int, rspHandlerItem* HandlerItem){

	if _, found := handlerMap[rspMsgId]; found == true{

		common.FatalLog(fmt.Sprintf("proto msg id is registered:%v", rspMsgId))
		return
	}

	rspHandlerItem.MsgId = rspMsgId
	rspHandlerItem.IsRequest = false
	handlerMap[rspMsgId] = rspHandlerItem

	reqHandlerItem, found := handlerMap[reqMsgId]

	if found{

		regMutualInfo(reqHandlerItem, rspHandlerItem)

	}

	if rspHandlerItem.MsgType != nil{
		handlerReverseMap[rspHandlerItem.MsgType] = rspMsgId
	}

}



func RegisterHandler(reqMsgId int, rspMsgId int, reqHandlerItem* HandlerItem, rspHandlerItem* HandlerItem) {

	if _, found := handlerMap[reqMsgId]; found == true{

		common.FatalLog(fmt.Sprintf("proto msg id is registered:%v", reqMsgId))
		return

	}

	if _, found := handlerMap[rspMsgId]; found == true{

		common.FatalLog(fmt.Sprintf("proto msg id is registered:%v", rspMsgId))
		return
	}

	reqHandlerItem.MsgId = reqMsgId
	rspHandlerItem.MsgId = rspMsgId

	reqHandlerItem.IsRequest = true
	rspHandlerItem.IsRequest = false

	handlerMap[reqMsgId] = reqHandlerItem
	handlerMap[rspMsgId] = rspHandlerItem

	regMutualInfo(reqHandlerItem, rspHandlerItem)

	if reqHandlerItem.MsgType != nil{
		handlerReverseMap[reqHandlerItem.MsgType] = reqMsgId
	}

	if rspHandlerItem.MsgType != nil{
		handlerReverseMap[rspHandlerItem.MsgType] = rspMsgId
	}
}


func regMutualInfo(reqItem, rspItem *HandlerItem){

	reqItem.PairRspMsgType = rspItem.MsgType
	rspItem.PairReqMsgId = reqItem.MsgId

}


/*
func SetMessageSeq(message interface{}, msgId int,seq int64) error{

	handlerItem, found := handlerMap[msgId]
	if found == false{
		return common.ErrorLog("handler item not found, msgid:", msgId)
	}

	handlerItem.MsgSetSeqFunc(message, seq)
	return nil
}*/



func DispatchProtoMessage(connector *Connector, message interface{}, msgId int, seq int64, handlerItem *HandlerItem) error{

	/*if handlerItem.MsgGetIdFunc == nil{
		return common.ErrorLog("msg get id func nil, id:", handlerItem.MsgId)
	}

	if handlerItem.MsgGetSeqFunc == nil{
		return common.ErrorLog("msg get seq func nil id:", handlerItem.MsgId)
	}

	seq, err := handlerItem.MsgGetSeqFunc(message)*/

	retCb, ok := connector.Server.callbacks.Get(int64(seq)).(*tcpCallback)
	if ok == true{
		retCb.ret <- message
		connector.Server.callbacks.Delete(seq)
		return nil
	}

	/*subMsgId,err := handlerItem.MsgGetIdFunc(message)
	if err != nil{
		return common.ErrorLog("get sub msg id null, ", handlerItem.MsgId)
	}*/

	subHandlerItem, ok := handlerMap[msgId]
	if ok != true || subHandlerItem == nil{
		return common.ErrorLog("dispatch cannot find handler by id:", msgId)
	}

	if subHandlerItem.Handler == nil{
		return common.ErrorLog("sub msg handler nil, id:", handlerItem.MsgId)
	}

	if err := subHandlerItem.Handler.Handle(connector, nil, message); err != nil{
		return errors.New(fmt.Sprintf("handle failed: %v", handlerItem.MsgId))
	}


	return nil
}




func (this *Connector) ObjRequestPackageDispatch(buf *common.MBuffer, parentHandlerItem *HandlerItem) error{

	reqHeader := &common.ObjRequestHeader{}

	if err := common.DecodeObjectPacket(buf, reqHeader); err != nil{
		return common.ErrorLog("decode header failed")
	}


	handlerItem, err := getMsgRegItemById(reqHeader.Id)
	if err != nil{
		return common.ErrorLog(" get second msg item faied parent", reqHeader.Id, " sec id:", reqHeader.Id)
	}


	var msg interface{}

	msg = reflect.New(handlerItem.MsgType).Interface()
	if err = common.DecodeObjectPacket(buf, msg); err != nil{
		return common.ErrorLog("decode body failed:", *reqHeader)
	}

	if err := handlerItem.Handler.Handle(this, reqHeader, msg); err != nil{
		return common.ErrorLog(fmt.Sprintf("handle failed: %v", handlerItem.MsgId))
	}

	return nil

}

func (this *Connector) ObjResponsePackageDispatch(buf *common.MBuffer, parentHandlerItem *HandlerItem) error{

	rspHeader := &common.ObjResponseHeader{}

	if err := common.DecodeObjectPacket(buf, rspHeader); err != nil{
		return common.ErrorLog("decode header failed")
	}

	handlerItem, err := getMsgRegItemById(rspHeader.Id)
	if err != nil{
		return common.ErrorLog(" get second msg item faied parent", rspHeader.Id, " sec id:", rspHeader.Id)
	}


	if !this.Server.callbacks.Contains(rspHeader.Seq){				// when no seq , not response handled
		//return common.ErrorLog("not rsp with this seq:", *rspHeader)

		msg := reflect.New(handlerItem.MsgType).Interface()				//服务器A发消息到B 可以只发请求或圆管（比如 被请求后很久再回答时不注册seq）
		if err = common.DecodeObjectPacket(buf, msg); err != nil{
			return common.ErrorLog("decode body failed:", *rspHeader)
		}

		if err := handlerItem.Handler.Handle(this, rspHeader, msg); err != nil{
			return common.ErrorLog(fmt.Sprintf("handle failed: %v", handlerItem.MsgId))
		}
		return nil
	}

	cb, ok := this.Server.callbacks.Get(rspHeader.Seq).(*tcpCallback)
	if ok == false{
		return common.ErrorLog("get rsp cb error")
	}


	defer func(){

		this.Server.callbacks.Delete(rspHeader.Seq)

	}()

	var msg interface{}

	if rspHeader.Err != 0{

		common.InfoLog("response is error", *rspHeader)

		close(cb.ret)

		return nil
	}


	if handlerItem.IsByPassSecondRspMsgDecoder{

		cb.ret <- common.NewBufferFromBuffer(buf)
		return nil
	}else{

		msg = reflect.New(handlerItem.MsgType).Interface()
		if err = common.DecodeObjectPacket(buf, msg); err != nil{
			return common.ErrorLog("decode body failed:", *rspHeader)
		}

	}

	cb.ret <- msg

	return nil

}
















