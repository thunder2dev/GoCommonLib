package tcpserver

import (
	//"time"
	"MarsXserver/common"
	"sync/atomic"
	"math"
	"fmt"
	"reflect"
	"time"
)

//call by go func

const (
	Default_Spy_Timeout = time.Second * 1000
	Default_File_Send_Sleep = time.Millisecond * 20
)


type ObjRequestCallbackFunc func(rsp interface{}, err error)
type ProtoRequestCallbackFunc func(rsp interface{}, err error)



func (this *TcpServer) WriteObjRequestMessageToServerByTypeSync(stype string, body interface{}, hashIds... int) (interface{}, error){

	retCh := make(chan interface{})

	go this.WriteObjRequestMessageToServerByType(stype, body, retCh, hashIds...)

	rsp, ok := <- retCh

	if ok == false{
		return nil, common.ErrorLog("response channel is closed")
	}

	if rsp == nil{
		return nil, common.ErrorLog("response is nil")
	}

	return rsp, nil

}


func (this *TcpServer) WriteObjRequestMessageToServerByTypeCallback(stype string, body interface{}, cb ObjRequestCallbackFunc, hashIds... int){


	go func(){

		rsp, err := this.WriteObjRequestMessageToServerByTypeSync(stype, body, hashIds...)

		if err != nil{
			cb(nil, err)
		}

		if rsp == nil{
			cb(nil, common.ErrorLog("response is nil"))
		}

		cb(rsp, nil)
	}()
}

/*
func (this *TcpServer) BroadcastObjRequestMessageToServerByType(stype string, body interface{}, ret_ chan interface{}) error{

	defer func(){
		if err := recover(); err != nil{
			common.ErrorLog("write msg to", stype, "failed", body, err)
		}
	}()

	msgId, err := getMsgIdByMsg(body)
	if err != nil{
		return common.ErrorLog("get msg id failed", err)
	}

	reqHeader := common.ObjRequestHeader{
		Id: msgId,
	}

	sids := this.dialerHub.GetAllSidByType(stype)

	if len(sids) == 0{
		return common.ErrorLog("no dialer for type", stype)
	}


	for _, sid := range sids{

		if err = this.WriteObjRequestMesageToServerById(sid, &reqHeader, body, common.MessageId_ObjRequestMessageId, ret_); err != nil{
			common.ErrorLog("broadcast has a error for ", sid, err)
		}
	}

	return err
}*/

//if ret is null, no callback
func (this *TcpServer) WriteObjRequestMessageToServerByType(stype string, body interface{}, ret_ chan interface{}, hashIds... int) error{

	defer func(){
		if err := recover(); err != nil{
			common.ErrorLog("write msg to", stype, "failed", body, err)
		}
	}()

	msgId, err := getMsgIdByMsg(body)
	if err != nil{
		return common.ErrorLog("get msg id failed", err)
	}

	reqHeader := common.ObjRequestHeader{
		Id: msgId,
	}

	dialer, err := this.dialerHub.GetHashedDialerByType(stype, hashIds...)
	if err != nil{
		close(ret_)
		return err
	}

	return this.WriteObjRequestMesageToServerById(dialer.sid, &reqHeader, body, common.MessageId_ObjRequestMessageId, ret_)
}


func (this *TcpServer) WriteObjRequestMesageToServerById(sid int, header *common.ObjRequestHeader, body interface{}, msgid int, ret_ chan interface{}) error{

	if ret_ != nil{
		atomic.AddInt64(&this.sendReqSeq, 1)
		if this.sendReqSeq == 0{
			atomic.AddInt64(&this.sendReqSeq, 1)
		}
		header.Seq = this.sendReqSeq
		this.callbacks.Set(this.sendReqSeq, &tcpCallback{
			ret: ret_,
		})
	}

	dialer, err := this.dialerHub.GetDialerBySid(sid)
	if err != nil{
		return common.ErrorLog("get dialer by sid failed, sid:", sid, err)
	}

	return dialer.WriteDoubleMessage(header, body, msgid)

}

func (co *Connector) WriteObjRequestMessage(message interface{}, ret_ chan interface{}) error{

	var err error

	if ret_ != nil{
		atomic.AddInt64(&co.Server.sendReqSeq, 1)
		if co.Server.sendReqSeq == 0{
			atomic.AddInt64(&co.Server.sendReqSeq, 1)
		}
		co.Server.callbacks.Set(co.Server.sendReqSeq, &tcpCallback{
			ret: ret_,
		})
	}

	msgId, err := getMsgIdByMsg(message)
	if err != nil{
		return common.ErrorLog("get msg id failed", err)
	}

	reqHeader := common.ObjRequestHeader{
		Id: msgId,
	}

	co.WriteDoubleMessage(reqHeader, message, common.MessageId_ObjRequestMessageId,)
	return nil

}



func (this *TcpServer) WriteProtoRequestMessageToServerByTypeCallback(stype string, body interface{}, cb ProtoRequestCallbackFunc, hashIds... int){

	go func(){

		rsp, err := this.WriteProtoRequestMessageToServerByTypeSync(stype, body, hashIds...)

		if err != nil{
			cb(nil, err)
		}

		if rsp == nil{
			cb(nil, common.ErrorLog("response is nil"))
		}

		cb(rsp, nil)
	}()
}



func (this *TcpServer) WriteProtoRequestMessageToServerByTypeSync(stype string, body interface{}, hashIds... int) (interface{}, error){

	retCh := make(chan interface{})

	go this.WriteProtoRequestMessageToServerByType(stype, body, retCh, hashIds...)

	rsp, ok := <- retCh

	if ok == false{
		return nil, common.ErrorLog("proto response channel is closed")
	}

	if rsp == nil{
		return nil, common.ErrorLog("proto response is nil")
	}

	return rsp, nil

}




func (this *TcpServer) WriteProtoRequestMessageToServerByType(stype string, message interface{}, ret_ chan interface{}, hashIds... int) error {

	dialer, err := this.dialerHub.GetHashedDialerByType(stype, hashIds...)
	if err != nil{
		close(ret_)
		return err
	}

	msgId, err := getMsgIdByMsg(message)
	if err != nil{
		return common.ErrorLog("proto get msg id failed", err)
	}


	return this.WriteProtoRequestMessageToServerById(dialer.sid, message, msgId, ret_)
}

//if ret == nil no callback
func (this *TcpServer) WriteProtoRequestMessageToServerById(sid int, message interface{}, msgid int, ret_ chan interface{}) error{

	//SetMessageSeq(message, msgid, this.sendReqSeq)

	if ret_ != nil{

		atomic.AddInt64(&this.sendReqSeq, 1)
		if this.sendReqSeq == 0{
			atomic.AddInt64(&this.sendReqSeq, 1)
		}

		this.callbacks.Set(this.sendReqSeq, &tcpCallback{
			ret: ret_,
		})
	}

	dialer, err := this.dialerHub.GetDialerBySid(sid)
	if err != nil{
		return common.ErrorLog("get dialer by sid failed, sid:", sid, err)
	}


	var buff *common.MBuffer
	if buff, err = encodeMessage(message, CPP_SERVER_MSG_TYPE_Req, this.sendReqSeq, msgid); err != nil{
		common.ErrorLog("encode message failed", err)
		return err
	}


	dialer.WriteBytes(buff.GetDataBuffer())
	return nil

}


func (co *Connector) WriteProtoRequestMessage(message interface{}, ret_ chan interface{}) error{

	var buff *common.MBuffer
	var err error

	if ret_ != nil{

		atomic.AddInt64(&co.Server.sendReqSeq, 1)
		if co.Server.sendReqSeq == 0{
			atomic.AddInt64(&co.Server.sendReqSeq, 1)
		}

		co.Server.callbacks.Set(co.Server.sendReqSeq, &tcpCallback{
			ret: ret_,
		})
	}

	msgId, err := getMsgIdByMsg(message)
	if err != nil{
		return common.ErrorLog("proto get msg id failed", err)
	}


	if buff, err = encodeMessage(message, CPP_SERVER_MSG_TYPE_Req, co.Server.sendReqSeq, msgId); err != nil{
		common.ErrorLog("encode message failed", err)
		return err
	}

	co.WriteBytes(buff.GetDataBuffer())
	return nil

}



func (co *Connector) WriteProtoResponseMessage(message interface{}, seq int64) error{
	var buff *common.MBuffer
	var err error

	msgId, err := getMsgIdByMsg(message)
	if err != nil{
		return common.ErrorLog("proto get msg id failed", err)
	}


	if buff, err = encodeMessage(message, CPP_SERVER_MSG_TYPE_Rsp, seq, msgId); err != nil{
		common.ErrorLog("encode message failed", err)
		return err
	}

	co.WriteBytes(buff.GetDataBuffer())

	return nil
}


/*
func (this *TcpServer) WriteMessageToServer(sid int, message interface{}, msgid int) error{

	return this.dialerHub.writeMessageToServerById(sid, message, msgid)
}*/


/*
func (co *Connector) WriteRequestMessage(message interface{}, msgId int, ret_ chan interface{}) error{
	var buff *MBuffer
	var err error
	if buff, err = encodeMessage(message, msgId); err != nil{
		common.ErrorLog("encode message failed", err)
		return err
	}

	atomic.AddInt64(&co.Server.sendReqSeq, 1)

	SetMessageSeq(message, msgId, co.Server.sendReqSeq)

	if ret_ != nil{
		co.Server.callbacks.Set(co.Server.sendReqSeq, &tcpCallback{
			ret: ret_,
		})
	}


	co.WriteBytes(buff.GetDataBuffer())

	return nil
}*/



func (co *Connector) WriteObjResponseMesage(reqHeader *common.ObjRequestHeader, errno int, body interface{}) error{

	msgId, err := getMsgIdByMsg(body)
	if err != nil{
		return common.ErrorLog("get msg id failed", err)
	}

	rspHeader := &common.ObjResponseHeader{
		Seq: 0,
		Err: errno,
		Id: msgId,
	}

	if reqHeader != nil{
		rspHeader.Seq = reqHeader.Seq
	}

	return co.WriteDoubleMessage(rspHeader, body, common.MessageId_ObjReponseMessageId)

}


func (co *Connector) WriteDoubleMessage(msg1, msg2 interface{}, msgId int) error{

	var buff *common.MBuffer
	var err error
	if buff, err = encodeDoubleMessage(msg1, msg2, msgId); err != nil{
		return common.ErrorLog("encode double message failed")
	}

	//common.InfoLog("send bytes:", buff.GetDataBuffer())
	co.WriteBytes(buff.GetDataBuffer())

	return nil
}


func (co *Connector) WriteSimpleMessage(message interface{}, msgId int) error{
	var buff *common.MBuffer
	var err error
	if buff, err = encodeMessage(message,0, 0, msgId); err != nil{
		common.ErrorLog("encode message failed", err)
		return err
	}

	co.WriteBytes(buff.GetDataBuffer())

	return nil
}




func (co *Connector) WriteBytes(bytes []byte){
	//todo close conn
	//control write and read num

	co.writeLock.Lock()
	defer co.writeLock.Unlock()

	//common.InfoLog("send bytes", bytes, " seq:", co.Server.sendReqSeq)

	var start, n int
	var err error
	for{
		//bytes = append(bytes, 12, 34)

		if n, err = co.conn.Write(bytes[start:]); err != nil{
			common.ErrorLog("dial write error", err, " sid:", co.Server.sid)
			return
		}

		/*
		time.Sleep(time.Second * 2)

		_, err = conn.conn.Write(bytes[10:15])

		time.Sleep(time.Second * 2)

		_, err = conn.conn.Write(bytes[15:])*/

		start += n

		if start > len(bytes){
			common.ErrorLog("write index exceeds bytes len")
		}

		if n == 0 || start == len(bytes){
			return
		}
	}

}


func (this *TcpServer) SendFileToServerByType(stype string, msg interface{},fileName string, data []byte) (interface{} , error){

	msgId, err := getMsgIdByMsg(msg)
	if err != nil{
		return nil, common.ErrorLog("get msg id failed", err)
	}


	hashId := common.GetHashNumByString(fileName)

	common.InfoLog("hash id for file:", fileName, hashId)

	dialer, err := this.dialerHub.GetHashedDialerByType(stype, int(hashId))
	if err != nil{
		return nil, err
	}

	return dialer.sendFile(msgId, msg, fileName, data)
}

/*
func (dh *DialerHub) sendFileToServerById(sid int, msgId int, fileName string, data []byte,retCh chan interface{}) error{

	conn, ok := dh.idDic[sid]
	if ok != true{
		common.ErrorLog("no such tcpserver", sid, " tcpserver map:", dh.idDic, " sid:", dh.server.sid)
		return errors.New("no such tcpserver")
	}

	return conn.SendFile(msgId, fileName, data, retCh)

}*/


func (co *Connector) SendObjErrorResponse(reqHeader *common.ObjRequestHeader, errCode int){

	rspItem, err := getMsgRegItemById(reqHeader.Id)
	if err != nil{
		common.ErrorLog(fmt.Sprintf("msgid does not exists:%v", reqHeader.Id))
		return
	}

	rsp := reflect.New(rspItem.PairRspMsgType).Interface()

	if err := co.WriteObjResponseMesage(reqHeader, errCode, rsp); err != nil{
		common.ErrorLog("write message rsp error", err)
	}
}


func (co *Connector) sendFileSpyRet(retCh chan interface{}, finishRet chan interface{}){

	var isLast bool

	timeoutCh := time.Tick(Default_Spy_Timeout)

	for{
		select {
		case retInf := <- retCh:
			if !isLast{
				header, ok  := retInf.(*FileResponseHeader)
				if ok == false{
					common.ErrorLog("receive none file response")
					finishRet <- nil
					return
				}

				common.InfoLog("send spy", *header)

				if header.Err != 0{
					common.ErrorLog("file response is err", header.Err)
					finishRet <- nil
					return
				}

				common.InfoLog("spy received state", header.State)

				if header.State == int(File_Trans_End){
					isLast = true
					break
				}


			}else{
				finishRet <- retInf
				return
			}
		case <- timeoutCh:
			common.ErrorLog("file time out")
			goto spyloop
		}

	}
spyloop:
	common.InfoLog("send spy closed")

	finishRet <- nil

}



func (co *Connector) sendFile(msgId int, msg interface{},fileName string, data []byte) (interface{} ,error){

	atomic.AddInt64(&co.Server.sendReqSeq, 1)
	seq := co.Server.sendReqSeq


	fileSize := len(data)
	count := int(math.Ceil(float64(fileSize)/Default_File_One_Piece_Length))

	var ret_  = make(chan interface{})
	var spyRet_ = make(chan interface{})
	defer close(spyRet_)
	defer close(ret_)

	go co.sendFileSpyRet(ret_, spyRet_)

	var spyRet interface{}
	var gotRet bool

loopEnd:
	for ii := 0; ii < count; ii++{

		select{
		case spyRet = <- spyRet_:
			close(spyRet_)
			gotRet = true
			goto loopEnd
		default:
		}

		var length int
		var offset = ii * Default_File_One_Piece_Length


		if (ii + 1) * Default_File_One_Piece_Length <= fileSize{
			length = Default_File_One_Piece_Length
		}else{
			length = fileSize - offset
		}

		var req *FileRequestHeader

		if ii == 0{
			req = &FileRequestHeader{
				Seq: seq,
				State: int(File_Trans_Start),	//1:start 2:ing 3:end
				MsgId: msgId,
				FileName: fileName,
				FileSize: fileSize,
				Offset: 0,
				Length: length,
				Sha1: "",
			}

			ctx := &FileSendContext{
				ret: ret_,
			}

			co.Server.fileSendContex.Set(seq, ctx)

			co.WriteDoubleMessage(req, msg, common.MessageId_FileRequest)

		}else if ii == count -1{
			req = &FileRequestHeader{
				Seq: seq,
				State: int(File_Trans_End),	//1:start 2:ing 3:end
				MsgId: msgId,
				FileName: fileName,
				FileSize: fileSize,
				Offset: offset,
				Length: length,
				Sha1: "",
			}

			co.WriteSimpleMessage(req, common.MessageId_FileRequest)

		}else{
			req = &FileRequestHeader{
				Seq: seq,
				State: int(File_Trans_ING),	//1:start 2:ing 3:end
				MsgId: msgId,
				FileName: fileName,
				FileSize: fileSize,
				Offset: offset,
				Length: length,
				Sha1: "",
			}

			co.WriteSimpleMessage(req, common.MessageId_FileRequest)
		}

		co.WriteBytes(data[offset:offset + length])          //do not choose msg because msg receive buffer cannot be too large

		time.Sleep(time.Millisecond * 10)

	}


	var spyRetFinal interface{}
	var ok bool
	if gotRet == false {
		spyRetFinal, ok = <-spyRet_
	}else {
		spyRetFinal = spyRet

	}

	if spyRetFinal == nil || ok == false{
		return nil, common.ErrorLog("get rsp msg nil", fileName)
	}else{
		return spyRetFinal, nil

	}

}



























