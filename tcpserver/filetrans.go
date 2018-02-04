package tcpserver

import (
	"os"
	"MarsXserver/common"
	"time"
	"reflect"
)

type File_Trans_Step int

const (
	_ File_Trans_Step = iota

	File_Trans_Start

	File_Trans_ING

	File_Trans_End
)

const (
	Default_File_Session_GC_Time = time.Second * 5
	Default_File_Session_Timeout = time.Second * 20
)


const (
	Default_File_One_Piece_Length = 3720
)



type FileRequestHeader struct{

	Seq	int64
	State	int	//1:start 2:ing 3:end
	MsgId 	int
	FileName string
	FileSize int
	Offset 	int
	Length	int
	Sha1	string
}

type FileRequestData struct {
	Seq	int64
	Data 	[]byte
}

type FileResponseHeader struct {
	Seq int64
	MsgId int
	State int //1:start 2:ing 3:end
	Err int
	Msg string
}


type FileHandlerItem interface {
	FileTransStartFunc(ctx *FileReceiveContext) (*os.File, error)
	FileTransIngFunc(ctx *FileReceiveContext) error
	FileTransEndFunc(ctx *FileReceiveContext) error
}


type BaseFileHandlerItem struct {
}

func (this *BaseFileHandlerItem) FileTransStartFunc(ctx *FileReceiveContext) (*os.File, error){
	return nil, common.ErrorLog(" start func not defined")
}

func (this *BaseFileHandlerItem) FileTransIngFunc(ctx *FileReceiveContext) error{
	return nil
}

func (this *BaseFileHandlerItem) FileTransEndFunc(ctx *FileReceiveContext) error{
	return nil
}


type FileReceiveContext struct {
	File	*os.File
	SavePath string
	Conn	*Connector
	LastReq	*FileRequestHeader
	StartMsg	interface{}
	RspMsg		interface{}
	LastReadTime time.Time
	Handler FileHandlerItem

}


type FileSendContext struct{
	ret chan interface{}
	LastSentTime time.Time
	Seq int
}





var (
	FileHandlers = make(map[int]FileHandlerItem)
)


func init(){
	RegisterHandler(
		common.MessageId_FileRequest,
		common.MessageId_FileResponse,
		&HandlerItem{
			MsgProtoType: MessageProtoType_FileRequest,
		},
		&HandlerItem{
			MsgProtoType: MessageProtoType_FileResponse,
		},
	)
}



func RegisterFileHandler(reqMsgid int, rspMsgId int, reqMsgType reflect.Type, rspMsgType reflect.Type, fileHandler FileHandlerItem){
	FileHandlers[reqMsgid] = fileHandler
	RegisterHandler(
		reqMsgid,
		rspMsgId,
		&HandlerItem{
			MsgType: reqMsgType,
		},
		&HandlerItem{
			MsgType: rspMsgType,
		},
	)
}

func (this *TcpServer)fileResponsePackageDispath(connector *Connector, buf *common.MBuffer) error{

	var err error
	var ok bool

	var seq int64 = 0
	var hasSeq bool = false
	var ctx *FileSendContext

	for{
		header := &FileResponseHeader{}
		if err = common.DecodeObjectPacket(buf, header); err != nil{
			err = common.ErrorLog("decode file response failed", err)
			break
		}

		seq = header.Seq
		hasSeq = true

		ctxInf := this.fileSendContex.Get(seq)

		if ctxInf == nil{
			err = common.ErrorLog("ctx not exists", seq)
			break
		}

		ctx, ok = ctxInf.(*FileSendContext)
		if ok == false{
			err = common.ErrorLog("not a receive ctx")
			break
		}

		ctx.ret <- header

		if header.State == int(File_Trans_End){
			msgId := header.MsgId

			msgItem, err := getMsgRegItemById(msgId)
			if err != nil{
				common.ErrorLog("file rsp end get msg item failed", msgId, err)
				break
			}

			msg := reflect.New(msgItem.MsgType).Interface()
			if err = common.DecodeObjectPacket(buf, msg); err != nil{
				err = common.ErrorLog("decode file end response msg failed",msgId, err)
				break
			}

			ctx.ret <- msg
		}

		break
	}

	if err != nil{

		if hasSeq{

			if ctx != nil{
				close(ctx.ret)
			}

			this.fileSendContex.Delete(seq)
		}

		return err

	}


	return nil

}


func (this *TcpServer)fileRequestPackageDispatch(connector *Connector, buf *common.MBuffer) error{

	var gErr error

	req := &FileRequestHeader{}

	for{
		if err := common.DecodeObjectPacket(buf, req); err != nil{
			break
		}

		if req.State == int(File_Trans_Start){
			msgId := req.MsgId

			item, err := getMsgRegItemById(msgId)
			if err != nil{
				gErr = common.ErrorLog("get msg item failed")
				break
			}

			newMsg := reflect.New(item.MsgType).Interface()
			rspMsg := reflect.New(item.PairRspMsgType).Interface()

			if err = common.DecodeObjectPacket(buf, newMsg); err != nil{
				gErr = common.ErrorLog("decode start second msg failed:", msgId)
				break
			}

			if gErr = this.fileTransStart(connector, req, newMsg, rspMsg); gErr != nil{
				break
			}

		}else if req.State == int(File_Trans_ING){

			if gErr = this.fileTransIng(connector, req); gErr != nil{
				break
			}
		}else if req.State == int(File_Trans_End){

			if gErr = this.fileTransEnd(connector, req); gErr != nil{
				break
			}

		}else{
			return common.ErrorLog("file request state error:", req.State)
		}

		break
	}


	if gErr != nil{
		this.fileError(connector, req.Seq, gErr.Error())
	}

	return gErr

}

func writeFileRsp(conn *Connector, ctx *FileReceiveContext) error{

	req := ctx.LastReq

	rspHeader := &FileResponseHeader{
		MsgId: req.MsgId,
		Err: 0,
		Msg: "",
		Seq: req.Seq,
		State: req.State,
	}

	if req.State == int(File_Trans_End){
		return conn.WriteDoubleMessage(
			rspHeader,
			ctx.RspMsg,
			common.MessageId_FileResponse,
		)

	}else{
		return conn.WriteSimpleMessage(rspHeader, common.MessageId_FileResponse)
	}
}




func (this *TcpServer) fileTransStart(conn *Connector, req *FileRequestHeader, startMsg interface{}, rspMsg interface{}) error{

	ctx := &FileReceiveContext{
		Conn: conn,
		LastReq: req,
		LastReadTime: common.GetTimeNow(),
	}

	if this.fileReceiveContex.Contains(req.Seq){

		return common.ErrorLog("seq exists seq:", req.Seq)
	}

	if req.Length <= 0{
		return common.ErrorLog("data length empty,", req.Length)
	}


	handler, ok := FileHandlers[req.MsgId]
	if ok == false{
		return common.ErrorLog("file msg id error:", req.MsgId)
	}

	if req.Offset > 0 || req.Offset < 0{
		return common.ErrorLog("start offset is not 0", req.MsgId)
	}

	ctx.StartMsg = startMsg
	ctx.RspMsg = rspMsg

	f, err := handler.FileTransStartFunc(ctx)
	if err != nil{
		return err
	}

	ctx.File = f
	ctx.Handler = handler
	bts := make([]byte, req.Length)

	if err := readBytes(conn.conn ,bts); err != nil{
		return common.ErrorLog("read data failed", err)
	}

	if wn, err := f.Write(bts); err != nil || wn != req.Length{
		return common.ErrorLog("write file failed")
	}

	this.fileReceiveContex.Set(req.Seq, ctx)

	if req.Length >= req.FileSize{
		common.InfoLog("file:", req.FileName, " len:", req.Length, " more than size:", req.FileSize)
		/*if err := handler.FileTransEndFunc(ctx); err != nil{
			return common.ErrorLog("file trans end failed", err)
		}*/
		if err := this.fileTransFinish(ctx, req); err != nil{
			return common.ErrorLog("file trans end failed", err)
		}
	}

	writeFileRsp(conn, ctx)

	return nil

}

func (this *TcpServer) fileTransIng(conn *Connector, req *FileRequestHeader) error{

	seq := req.Seq
	ctx, ok := this.fileReceiveContex.Get(seq).(*FileReceiveContext)
	if ok != true{
		return  common.ErrorLog("file hub no seq, seq", seq)

	}

	if req.Length <= 0{
		return common.ErrorLog("data length empty,", req.Length)
	}

	if ctx.LastReq.FileName != req.FileName || ctx.LastReq.MsgId != req.MsgId{
		return common.ErrorLog(" request file mismatched")
	}

	expectOffset := ctx.LastReq.Offset + ctx.LastReq.Length

	if expectOffset != req.Offset{
		return common.ErrorLog("file offset error, expect:", expectOffset, "curr offset:", req.Offset)
	}

	if req.Offset + req.Length> req.FileSize{

		return common.ErrorLog(" curr req exceeds file size, expect:", req.Offset + req.Length, " size:", req.FileSize)
	}

	bts := make([]byte, req.Length)

	if err := readBytes(conn.conn ,bts); err != nil{
		return common.ErrorLog("read data failed", err)
	}

	if wn, err := ctx.File.Write(bts); err != nil || wn != req.Length{
		return common.ErrorLog("write data failed", err)
	}

	ctx.LastReq = req

	if err := ctx.Handler.FileTransIngFunc(ctx); err != nil{
		return err
	}

	writeFileRsp(conn, ctx)

	return nil
}

func (this *TcpServer) fileTransEnd(conn *Connector, req *FileRequestHeader) error{

	seq := req.Seq
	ctx, ok := this.fileReceiveContex.Get(seq).(*FileReceiveContext)
	if ok != true{
		return  common.ErrorLog("file hub no seq, seq", seq)

	}

	if req.Length <= 0{
		return common.ErrorLog("data length empty,", req.Length)
	}

	if ctx.LastReq.FileName != req.FileName || ctx.LastReq.MsgId != req.MsgId{
		return common.ErrorLog(" request file mismatched")
	}

	expectOffset := ctx.LastReq.Offset + ctx.LastReq.Length

	if expectOffset != req.Offset{
		return common.ErrorLog("file offset error, expect:", expectOffset, "curr offset:", req.Offset)
	}

	if req.Offset + req.Length != req.FileSize{

		return common.ErrorLog(" end req is not equal to file size, expect:", req.Offset + req.Length, " size:", req.FileSize)
	}

	bts := make([]byte, req.Length)

	if err := readBytes(conn.conn ,bts); err != nil{
		return common.ErrorLog("read data failed", err)
	}

	if wn, err := ctx.File.Write(bts); err != nil || wn != req.Length{
		return common.ErrorLog("write data failed", err)
	}

	if err:= this.fileTransFinish(ctx, req); err != nil{
		return err
	}

	writeFileRsp(conn, ctx)
	return nil
}



func (this *TcpServer) fileTransFinish(ctx *FileReceiveContext, req *FileRequestHeader) error {


	ctx.LastReq = req
	ctx.LastReq.State = int(File_Trans_End)
	ctx.File.Sync()
	ctx.File.Close()

	if err := ctx.Handler.FileTransEndFunc(ctx); err != nil{
		return err
	}

	this.fileReceiveContex.Delete(req.Seq)

	return nil

}



func (this *TcpServer) fileError(conn *Connector, seq int64, errMsg string){

	conn.WriteSimpleMessage(&FileResponseHeader{
		Err: -1,
		Msg: errMsg,
		Seq: seq,
	}, common.MessageId_FileResponse)


	ctxInf := this.fileReceiveContex.Get(seq)
	if ctxInf == nil{
		return
	}

	ctx, ok := ctxInf.(*FileReceiveContext)
	if ok == false{
		return
	}

	if ctx.File != nil {
		ctx.File.Close()
	}

	this.fileReceiveContex.Delete(seq)

}





func (this *TcpServer) fileSessionGC(){

	if this.fileReceiveContex.Count() <= 0 {
		return
	}

	common.InfoLog("File Session gc is running")

	keys := this.fileReceiveContex.Keys()

	delKeys := make([]interface{}, 0)

	for _, key := range keys{
		val := this.fileReceiveContex.Get(key)
		ctx := val.(FileReceiveContext)
		if common.GetTimeNow().Sub(ctx.LastReadTime) > Default_File_Session_Timeout{
			delKeys = append(delKeys, key)
			if ctx.File != nil{
				ctx.File.Close()
			}
		}
	}

	for _, dk := range delKeys{

		this.fileReceiveContex.Delete(dk)

	}


	time.AfterFunc(Default_File_Session_GC_Time, func(){ this.fileSessionGC()})

}



























