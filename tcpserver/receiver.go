package tcpserver

import (
	"fmt"
	"net"
	"github.com/golang/protobuf/proto"
	"reflect"
	"strconv"
	"errors"
	"MarsXserver/common"
)


const (
	MSG_LEN_AND_TYPE_BYTES = 12

)

type CPP_SERVER_MSG_TYPE uint32

const (
	_ CPP_SERVER_MSG_TYPE = iota
	CPP_SERVER_MSG_TYPE_Req
	CPP_SERVER_MSG_TYPE_Rsp
	CPP_SERVER_MSG_TYPE_Chunk
)



func handleClientConnection(svr *TcpServer, conn net.Conn){

	ip, port, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil{
		common.ErrorLog("split ip port failed", conn.RemoteAddr())
		return
	}

	port_i, err := strconv.ParseInt(port, 10, 32)
	if err != nil{
		common.ErrorLog("parse int failed", port_i)
		return
	}

	common.InfoLog("new conn local port:", svr.port, "remote port:", port_i)

	connector := NewConnector(ip, int(port_i), conn, svr)

	svr.connMgr.AddConn(connector)

	svr.eventScheduler.upChan <- connector

	ConnectionReadHandler(connector)

	svr.connMgr.RemoveConn(connector)

	svr.eventScheduler.downChan <- connector     //todo connection closed inform tcpserver

}


func ConnectionReadHandler(conn *Connector){

	defer func(){
		conn.conn.Close()
		if err := recover(); err!= nil{
			common.ErrorLog("panic:", err)
		}
	}()

	var buf = common.NewBuffer()

connectionReadLoop:
	for{
		select {
		case <- conn.Server.serverCloseChannel:
			common.InfoLog("conn is stopped by tcpserver:", conn.uid, conn.ip)
			break connectionReadLoop
		case <- conn.closeChannel:
			common.InfoLog("conn is stopped by channel", conn.uid, conn.ip)
			break connectionReadLoop
		default:
		}

		buf.Reset()


		msgLen, _, msgId, err := readMsgLength(conn.conn, buf)
		if err != nil{
			common.ErrorLog("read msg len failed sid:", conn.Server.sid, " ", conn.conn.RemoteAddr(), " err:", err)
			break
		}

		if msgLen > buf.Capcity(){
			common.ErrorLog("msg len is error:", msgLen)
			break
		}

		//todo msg md5

		//todo msg len type check

		msgItem, err := getMsgRegItemById(msgId)
		if err != nil{
			common.ErrorLog(fmt.Sprintf("msgid does not exists:%v", msgId))
			break
		}


		if err = readBody(conn.conn, msgLen, buf); err != nil{
			common.ErrorLog("read body failed", err)
			break
		}

		var msg interface{}
		var protoSeq int64

		if msgItem.MsgProtoType == MessageProtoType_Proto{

			msg, protoSeq, err = getProtoMsgFromBuff(msgLen, buf, msgItem.MsgType)
			if err != nil{
				common.ErrorLog("get proto failed sid:", conn.Server.sid, " ", msgId)
			}

		}else if msgItem.MsgProtoType == MessageProtoType_ObjRequest{

			err = conn.ObjRequestPackageDispatch (buf, msgItem)
			if err != nil{
				common.ErrorLog("dispatch obj request msg failed sid:", conn.Server.sid, " ", msgId)
			}
			continue

		}else if msgItem.MsgProtoType == MessageProtoType_ObjResponse{

			err := conn.ObjResponsePackageDispatch(buf, msgItem)
			if err != nil{
				common.ErrorLog(" dispatch obj response failed, sid:", conn.Server.sid, " ", msgId)
			}
			continue

		}else if msgItem.MsgProtoType == MessageProtoType_FileRequest{

			if err := conn.Server.fileRequestPackageDispatch(conn, buf); err != nil{
				common.ErrorLog("file package dispatch failed")
			}
			continue

		}else if msgItem.MsgProtoType == MessageProtoType_FileResponse{

			if err := conn.Server.fileResponsePackageDispath(conn, buf); err != nil{
				common.ErrorLog("file response dispatch failed")
			}
			continue
		}else{
			common.ErrorLog("msg proto type is error:")
			break
		}

		//todo handle in new channel
		err = DispatchProtoMessage(conn, msg, msgId, protoSeq, msgItem)
		if err != nil{
			common.ErrorLog(fmt.Sprintf("dispatcher handle failed %v", msgId))
			break
		}
	}

	if conn.noRedial{
		return
	}

	sid := conn.sid

	if sid <= 0{
		common.ErrorLog("sid is not a dialer", sid)
		return
	}

	dialerItem, err := conn.Server.configData.GetServerItemById(sid)
	if err != nil{
		common.ErrorLog("")
		return

	}

	newDialer := NewDialerConnector(dialerItem, conn.Server)

	common.InfoLog("server sid:", sid, " is disconnected from curr server")

	dialOkCh := make(chan int)
	defer close(dialOkCh)

	go conn.Server.dialerHub.repeatDial(newDialer, nil, dialOkCh)

	dialOk := <-dialOkCh

	if dialOk != 0{
		common.ErrorLog("dial failed", )
		conn.Server.dialerHub.OfflineDialer(newDialer)

		return
	}else{
		common.InfoLog("dial sucess, remote:", newDialer.port)
	}

	common.InfoLog("receiver end:", conn.port)

}






func readMsgLength(conn net.Conn, buf *common.MBuffer) (msgLen int, cppMsgCat uint32, msgType int, err error){

	for{
		//todo read timeout

		if buf.Capcity() <= 0{
			return 0, 0,0, errors.New("buf space is full")
		}

		n, err := conn.Read(buf.GetAvailableBuffer(MSG_LEN_AND_TYPE_BYTES))

		if err != nil{
			return 0,0, 0, err
		}

		err = buf.SetHaveSupply(n)

		if err != nil{
			return 0,0, 0, err
		}

		if buf.Length() < MSG_LEN_AND_TYPE_BYTES{
			continue
		}

		msgLen, err = buf.ReadInt()

		if err != nil{
			return 0,0,0, err
		}

		cppMsgCat, err = buf.ReadUint()

		if err != nil{
			return 0, 0, 0, err
		}


		msgType, err = buf.ReadInt()

		return msgLen, cppMsgCat, msgType, nil
	}
}



func readBody(conn net.Conn, bodyLen int, buf *common.MBuffer) error{  //todo timeout and cancel

	for ; buf.Length() < bodyLen; {
		//todo read timeout
		if buf.Capcity() <= 0{
			return errors.New("buf space is full")
		}

		n, err := conn.Read(buf.GetAvailableBuffer(bodyLen))
		if err != nil{
			return err
		}

		err = buf.SetHaveSupply(n)
		if err != nil{
			return err
		}

		if buf.Length() < bodyLen{
			continue
		}
	}

	return nil
}


func readBytes(conn net.Conn, bts []byte) error{

	dataLen := cap(bts)
	haveRead := 0

	for ; haveRead < dataLen; {
		//todo read timeout

		n, err := conn.Read(bts[haveRead:])
		if err != nil{
			return err
		}

		haveRead += n

		if haveRead < dataLen{
			continue
		}
	}

	return nil


}



func getProtoMsgFromBuff(bodyLen int, buf *common.MBuffer, msgType reflect.Type) (protoMsg interface{}, seq int64, err error){

	if buf.Length() < bodyLen + 4{
		return nil, -1, common.ErrorLog("buf length ", buf.Length(), " is less than ", bodyLen + 4)
	}

	seq, err = buf.ReadLong()
	if err != nil{
		return nil, -1, err
	}

	databytes, err := buf.ReadBytes(bodyLen)
	if err != nil{
		return nil, -1,  err
	}

	msgPtr := reflect.New(msgType).Interface()
	message, check :=  msgPtr.(proto.Message)
	if check == false{
		common.ErrorLog("cannot type to message:" + msgType.String())
		return nil, -1, err
	}

	err = proto.Unmarshal(databytes, message)
	if err != nil{
		common.ErrorLog("unmarshal failed for msg:" + msgType.String())
		return  nil, -1, err
	}

	return message, seq , err
}

























