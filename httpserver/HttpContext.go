package httpserver

import (
	"net/http"
	"github.com/gorilla/websocket"
)

type HttpContext struct{

	Rsp    http.ResponseWriter
	Req    *http.Request
	Ws	*websocket.Conn
	WsRouter *WebSocketRouter
	WsBody 	string

	Server *HttpServer

	Input *HttpInput
	Output *HttpOutput


	Session *SessionStore

}


func (this *HttpContext) Redirect(status int, newUrl string){

	this.Output.SetHeader("Location", newUrl)

}



