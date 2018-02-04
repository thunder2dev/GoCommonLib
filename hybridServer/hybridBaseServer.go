package hybridServer

import (
	"MarsXserver/tcpserver"
	"MarsXserver/httpserver"
	"MarsXserver/orm"
	"MarsXserver/common"
	"MarsXserver/bridge"
	"MarsXserver/redmodule"
	"time"
	"os"
	"os/signal"
	"syscall"
)

const (

	_ bridge.PipeRemoteType	= iota
	PipeType_Tcp
	PipeType_Http
)



type HybridBaseServer struct {

	TServer *tcpserver.TcpServer
	HServer *httpserver.HttpServer
	Orm     *orm.XOrm
	Red             *redmodule.XRedis
	Observer	*bridge.ObserverManager

	gcInfoTicker 	*GcInfoTicker

	closeSignal chan os.Signal
	CloseChan   chan struct{}

}


func (this *HybridBaseServer) InitBaseServer(name string, rootContext interface{}, tconfig *tcpserver.TcpConfigData, hconfig *httpserver.HttpConfigData, ormConfig *orm.OrmConfigData, redConfig *redmodule.RedisConfigData) error{

	this.CloseChan = make(chan struct{})

	this.closeSignal = make(chan os.Signal)
	signal.Notify(this.closeSignal, syscall.SIGINT, syscall.SIGTERM)

	go this.WaitForSignal()

	this.gcInfoTicker = NewGcInfoTicker(name)

	this.Observer = bridge.NewObserverManager(tconfig.Sid)

	morm, err := orm.NewOrm(ormConfig)
	if err != nil{
		return common.ErrorLog(err)
	}

	this.Orm = morm

	this.Red = redmodule.NewXRedis(redConfig)

	tserver, err := tcpserver.NewTcpServer(name, tconfig, morm, this.Red)
	if err != nil{
		return err
	}

	this.TServer = tserver
	this.TServer.Observer = this.Observer

	this.TServer.GetParentContext = func() interface{}{
		return rootContext
	}

	if hconfig != nil && hconfig.Sid > 0{
		hserver := httpserver.NewHttpServer(name, hconfig, morm)
		hserver.Pipe.RegisterRemote(PipeType_Tcp, tserver.Pipe)
		tserver.Pipe.RegisterRemote(PipeType_Http, hserver.Pipe)
		this.HServer = hserver
		this.HServer.Observer = this.Observer


		tserver.Pipe = bridge.NewPipeLine()
		hserver.Pipe = bridge.NewPipeLine()
		tserver.Pipe.RegisterRemote(PipeType_Http, hserver.Pipe)
		hserver.Pipe.RegisterRemote(PipeType_Tcp, tserver.Pipe)

		hserver.GetParentContext = func() interface{}{
			return rootContext
		}
	}


	return nil
}

func (this *HybridBaseServer) Close(){
	close(this.CloseChan)

	this.HServer.Close()
	this.TServer.Close()

}


func (this *HybridBaseServer) WaitForSignal(){

	select{
		case <-this.closeSignal:
			this.Close()
			return
		case <-this.CloseChan:
			return
	}

}


func (this *HybridBaseServer) RunBaseServer(blockMode bool) error{


	if this.Orm.IsServer{
		if err := this.Orm.CreateAll(); err != nil{
			return common.ErrorLog("create tables failed")
		}
	}


	if this.Red != nil && this.Red.IsServer{

		if err := this.Red.Connect(); err != nil{
			return common.ErrorLog("red connect failed", err)
		}

	}

	go this.gcInfoTicker.TickRun()

	if blockMode == false{
		this.TServer.Run(false)

		if this.HServer != nil{
			this.HServer.Run(false)
		}
	}else if this.HServer != nil{
		this.TServer.Run(false)
		this.HServer.Run(true)
	}else{
		this.TServer.Run(true)
	}


	return nil

}


func (this *HybridBaseServer) AddGcInfoTask(name string, duration time.Duration, handler func()){

	this.gcInfoTicker.AddGcInfo(name, duration, handler)


}



func (this *HybridBaseServer) TcpSendFileFuncByType(dialerType string , msg interface{},fileName string, data []byte) (interface{}, error){

	return this.TServer.SendFileToServerByType(dialerType,
		msg,
		fileName,
		data)

}


func (this *HybridBaseServer) OrmSendFuncByType(dialerType string , expr *orm.XOrmEprData, retCh chan interface{}){

	common.InfoLog("send func:", expr.ModelName, " optype", expr.OpType)
	this.TServer.WriteObjRequestMessageToServerByType(
		dialerType,
		&orm.DBExprRequest{
			Expr: expr,
		},
		retCh,
		0,  //todo write hash for db
	)

}

func (this* HybridBaseServer) RedSendFuncByType(dialerType string, req *redmodule.RedDataRequest, retCh chan interface{}){

	common.InfoLog("red send func:", req.StructName, " op:", req.Op)
	this.TServer.WriteObjRequestMessageToServerByType(
		dialerType,
		req,
		retCh,
		0, //todo write hash for red
	)
}



func (this *HybridBaseServer) RegisterHttpHandlers(handlers map[string]interface{}){

	if this.HServer != nil{
		this.HServer.RegisterRouter(handlers)

	}

}



func (this *HybridBaseServer) RegisterTcpDialerFinishedHandler(handler tcpserver.TcpDialerFinishedFunc){

	this.TServer.TcpDialerFinishedFunc = handler

}







/*
func (this *HybridBaseServer) SetParentFunc(getParentContextFunc GetParentContextFunc){

	this.getParentFunc = getParentContextFunc


}*/



























