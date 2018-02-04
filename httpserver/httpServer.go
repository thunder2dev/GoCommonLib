package httpserver

import (
	"net/http"
	"fmt"
	"strconv"
	"time"
	"MarsXserver/bridge"
	"MarsXserver/common"
	"MarsXserver/orm"
)


type HttpServer struct{

	sid int
	name string
	port int

	server          *http.Server
	router          *HttpRouterHub
	sessionManager  *SessionManager
	templateManager *HttpTemplateMgr
	WebSocketHub    *WebSocketHub
	HijackHub		*HijackHub
	Orm		*orm.XOrm

	Observer *bridge.ObserverManager    //from hybrid

	GetParentContext func()interface{}

	AuthFunc func(handler HttpHandler, ctx *HttpContext) (bool, error)

	Pipe *bridge.PipeLine

	closeChannel		chan struct{}
}


/*func NewSimpleHttpServer(sid_ int, name_ string, port_ int, orm_ *orm.XOrm) *HttpServer{

	cer, err := tls.LoadX509KeyPair("server.crt", "server.key")
	if err != nil{
		common.ErrorLog("server", name_, sid_, "https tlsConfig err", err)
		return nil
	}

	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cer}}

	_router := NewRouterHub()
	server := http.Server{
		Addr:         fmt.Sprintf(":%s", port_),
		Handler:      _router,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 2 * time.Second,
		TLSConfig:    tlsConfig,
	}


	httpServer := &HttpServer{
		sid:		sid_,
		name:            name_,
		port:            port_,

		closeChannel:	make(chan struct{}),

		server:          &server,
		router:          _router,
		sessionManager:  nil,
		templateManager: nil,
		WebSocketHub:    nil,
		HijackHub: 		 nil,
		Orm: 		orm_,

		Pipe: bridge.NewPipeLine(),
	}

	return httpServer

}*/


func NewHttpServer(_name string, configData *HttpConfigData, orm_ *orm.XOrm) *HttpServer{
	/*
	cer, err := tls.LoadX509KeyPair("server.crt", "server.key")
	if err != nil{
		common.ErrorLog("server", configData.Name, configData.Sid, "https tlsConfig err", err)
		return nil
	}

	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cer}}*/

	_router := NewRouterHub()
	_sessionManager := NewSessionManager(common.DefaultCookieName)

	var _templateManager *HttpTemplateMgr = nil

	if len(configData.TemplatePath) > 0{
		_templateManager = NewHttpTemplateMgr(configData.TemplatePath)
	}

	_wsManager := &WebSocketHub{AllWs: common.NewMMap()}
	_hijackHub := NewHijackHub()

	server := http.Server{
		Addr: fmt.Sprintf(":%s", strconv.FormatInt(int64(configData.Port), 10)),
		Handler: _router,
		ReadTimeout: 3 * time.Second,
		WriteTimeout: 2 * time.Second,
		//TLSConfig:    tlsConfig,
	}

	httpServer := HttpServer{
		sid:		configData.Sid,
		name:            _name,
		port:            configData.Port,

		closeChannel:	make(chan struct{}),

		server:          &server,
		router:          _router,
		sessionManager:  _sessionManager,
		templateManager: _templateManager,
		WebSocketHub:    _wsManager,
		HijackHub:		 _hijackHub,
		Orm: 		orm_,

		Pipe: bridge.NewPipeLine(),
	}

	_router.server = &httpServer

	for _, sroute := range configData.StaticPaths.Paths{

		_router.RegisterStaticFolder(sroute.Src, sroute.Dst)

	}

	return &httpServer

}




func (this *HttpServer) RegisterAuthFunc(authFunc func(handler HttpHandler, ctx *HttpContext)(bool, error)){

	this.AuthFunc = authFunc

}



func (this *HttpServer) RegisterRouter(handlers map[string]interface{}) error{

	for path, handler := range handlers{

		this.router.RegisterRouter(path, handler)

	}

	/*if _, ok := this.router.routes[path]; ok == true{
		return common.ErrorLog("route is registered:", path)
	}*/

	return nil
}


func (this *HttpServer) Run(blockMode bool){ //too close cannot close websocket

	if this.templateManager != nil{

		this.templateManager.InitTemplates()

	}

	if blockMode == false{
		go this.server.ListenAndServe()
	}else{
		this.server.ListenAndServe()
	}
}


func (this *HttpServer) Close(){

	if !common.IsStructClosed(this.closeChannel){

		this.server.Close()
		close(this.closeChannel)

		common.InfoLog( this.name + " hserver closed")
	}
}


























