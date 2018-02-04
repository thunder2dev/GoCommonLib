package tcpserver

import(
	//"fmt"
	//"html/template"
	"net"
	//"os"
	//"path"
	//"runtime"
	//"strconv"
	"log"
	"strconv"
	"MarsXserver/common"
	"MarsXserver/bridge"
	"time"
	"MarsXserver/orm"
	"MarsXserver/redmodule"
)


const VERSION = 1.0

const(

	_ bridge.ObserverType = iota + common.Root_TcpObserverType
	ObserverType_DialerFinished

)


type TcpSendFileFunc func(msg interface{},fileName string, data []byte) error
type TcpDialerFinishedFunc func()

type TcpServer struct{

	sid int
	name string
	port int

	configData *TcpConfigData

	eventScheduler *EventScheduler

	dialerHub *DialerHub

	AllDialers *common.MSet   //todo 现在只用来A接入B的时候,在A处验证，在B处也应该加上

	listener net.Listener

	connMgr *ConnectorMgr

	sendReqSeq int64

	callbacks *common.MMap

	fileSendContex 	*common.MMap
	fileReceiveContex *common.MMap

	Pipe *bridge.PipeLine

	Orm  *orm.XOrm
	Red	 *redmodule.XRedis

	Observer *bridge.ObserverManager   //from hybrid

	GetParentContext func()interface{}
	TcpSendFileFunc TcpSendFileFunc

	TcpDialerFinishedFunc TcpDialerFinishedFunc


	serverCloseChannel chan struct{}

}


type tcpCallback struct {
	timeout time.Time
	ret chan interface{}
}


func NewTcpServerDefault(sid_ int, name_ string, port_ int, configData *TcpConfigData, orm_ *orm.XOrm, _red *redmodule.XRedis) (svr *TcpServer, err error){

	srv := &TcpServer{sid: sid_, name: name_, port: port_}
	srv.configData = configData
	if configData == nil{
		common.InfoLog("Server sid:", sid_, " config is null")
	}
	srv.eventScheduler = NewEventScheduler(srv)
	srv.dialerHub = NewDialerHub(srv)
	srv.connMgr = NewConnectionMgr(srv)

	srv.serverCloseChannel = make(chan struct{})
	srv.Pipe = bridge.NewPipeLine()
	srv.callbacks = common.NewMMap()
	srv.Orm = orm_
	srv.Red = _red
	srv.AllDialers = common.NewMSet()
	srv.fileReceiveContex = common.NewMMap()
	srv.fileSendContex = common.NewMMap()


	return srv, nil
}


func NewTcpServer(_name string, configData *TcpConfigData, orm_ *orm.XOrm, _red *redmodule.XRedis) (svr *TcpServer, err error){
	serverItem, err := configData.GetCurrServerItem()
	if err != nil{
		common.ErrorLog("get curr tcpserver failed", err)
		return nil, err
	}

	common.InfoLog("tcpserver item:", *serverItem, " config:", *configData)

	return  NewTcpServerDefault(serverItem.Sid, _name, serverItem.Port, configData, orm_, _red)

}


func (this *TcpServer) RegitsterSendFileFunc(tcpSendFileFunc TcpSendFileFunc){

	this.TcpSendFileFunc = tcpSendFileFunc
}


func MakeLaddrStr(_port int) string{
	return ":" + strconv.Itoa(_port)
}


func (this *TcpServer) SetLegalDialerNames(names []string){

	for _, name := range names{
		this.AllDialers.Insert(name)
	}

}


func (this *TcpServer) isServerClosed() bool{
	select{
	case <-this.serverCloseChannel:
		return true
	default:
		return false
	}

}

func (this *TcpServer) Close(){

	this.listener.Close()

	if common.IsStructClosed(this.serverCloseChannel) == false{
		close(this.serverCloseChannel)

		common.InfoLog(this.name + " tcpserver closed")
	}
}


func (this *TcpServer) Run(blockMode bool){

	defer func (){

		if err := recover(); err != nil{
			this.Close()
		}

	}()

	addrStr := MakeLaddrStr(this.port)

	listener, err := net.Listen("tcp", addrStr)
	if err != nil{
		common.FatalLog(err)
		return
	}

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil{
		common.FatalLog(err)
		return
	}

	common.InfoLog("listen at:", port)

	this.listener = listener

	go this.eventScheduler.Scheduler()


	go func(){
		for{
			if this.isServerClosed(){
				common.InfoLog("listen loop stopped")
				break
			}

			conn, err := listener.Accept()
			if err != nil{
				log.Println(err)
				continue
			}

			go handleClientConnection(this, conn)
		}
	}()

	if this.configData == nil || len(this.configData.Dialers.Dialer) <= 0{
		common.InfoLog("no dialers")

	}else{

		var dialerConnectors = make([]*Connector, len(this.configData.Dialers.Dialer))

		for idx, dialerId := range this.configData.Dialers.Dialer{

			dialerItem, err := this.configData.GetServerItemById(dialerId)
			if err != nil{
				common.ErrorLog("")
			}

			dialerConnectors[idx] = NewDialerConnector(dialerItem, this)
			if this.AllDialers.Contains(dialerItem.SType) == false{
				common.ErrorLog("dialer type is error:", dialerItem.SType)
				return
			}
		}

		go func(){
			if this.TcpDialerFinishedFunc != nil{
				this.Observer.RegisterObserver(ObserverType_DialerFinished, false, nil, func(ctx *bridge.ObserverContext){
					this.TcpDialerFinishedFunc()
				})
			}

			if err = this.dialerHub.Run(dialerConnectors); err != nil{
				common.ErrorLog("run dialer failed", err)
				return
			}

		}()

	}

	this.fileSessionGC()


	if blockMode{
		<-this.serverCloseChannel
		common.InfoLog(this.name + " tcpserver closed")
	}

	common.InfoLog("tcpserver sid:", this.sid, " is unblocked")
}




func (this *TcpServer) GetDialerNumByType(stype string) int{

	return this.dialerHub.getDialerNumByType(stype)

}








