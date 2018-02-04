package httpserver

import (
	"time"
	"github.com/gorilla/websocket"
	"MarsXserver/common"
	"io/ioutil"
	"encoding/json"
)

const(
	DefaultWSWriteWait = 10 * time.Second
	DefaultWSReadWait = 60 * time.Second
	DefaultPingPeriod = (DefaultWSReadWait * 9)/10
	DefaultMaxMessageSize = 512
)


type WebSocketHub struct{

	AllWs *common.MMap
}

type WebSocketMessage struct {
	MsgName string
	Payload string
}



type WebSocketRouter struct{
	Id 	int
	LastRW	time.Time
	Conn	*websocket.Conn
	hub	*WebSocketHub
	SendChannel chan *WebSocketMessage
	ctx 	*HttpContext
}


func (this *WebSocketHub) NewWebSocketRouter(id_ int, ctx_ *HttpContext) *WebSocketRouter{

	ws := &WebSocketRouter{
		Id: 	id_,
		LastRW: common.GetTimeNow(),
		Conn: ctx_.Ws,
		hub: this,
		SendChannel:make(chan *WebSocketMessage),
		ctx: ctx_,
	}

	this.AllWs.Set(id_, ws)

	return ws
}

func (this *WebSocketHub) SendMessage(id int, msgName string, msg interface{}) error{

	router := this.AllWs.Get(id).(*WebSocketRouter)

	msgData, err := json.Marshal(msg)
	if err != nil{
		return common.ErrorLog("marshal msg failed, id:", id)
	}

	message := WebSocketMessage{
		MsgName:msgName,
		Payload:string(msgData),
	}

	router.SendChannel <- &message

	return nil
}


func (this *WebSocketRouter) Run(){

	go this.Send()
	this.Receive()

}


func (this *WebSocketRouter) Receive(){
	defer func(){
		this.Conn.Close()
	}()

	this.Conn.SetReadLimit(DefaultMaxMessageSize)
	this.Conn.SetReadDeadline(common.GetTimeNow().Add(DefaultWSReadWait))

	for{
		op, r, err := this.Conn.NextReader()
		if err != nil{
			break
		}

		switch op {
		case websocket.PongMessage:
			this.Conn.SetReadDeadline(common.GetTimeNow().Add(DefaultWSReadWait))
			common.InfoLog("get pong msg")
		case websocket.TextMessage:
			message, err := ioutil.ReadAll(r)
			if err != nil{
				break
			}

			msg := WebSocketMessage{}

			ss := string(message)

			common.InfoLog("ss:", ss)

			if err = json.Unmarshal(message, &msg); err != nil{
				common.ErrorLog("unmarshal id.", this.Id, " message failed", err)
				break
			}

			this.ctx.Server.router.WebsocketRoute(this.ctx, msg.MsgName, msg.Payload)
		}


	}
}

func (this *WebSocketRouter) write(op int, payload []byte) error{

	this.Conn.SetWriteDeadline(common.GetTimeNow().Add(DefaultWSWriteWait))
	return this.Conn.WriteMessage(op, payload)

}


func (this *WebSocketRouter) WriteMessage(msgName string, msg interface{}) error{

	msgData, err := json.Marshal(msg)
	if err != nil{
		return common.ErrorLog("marshal msg failed, id:", this.Id)
	}

	message := WebSocketMessage{
		MsgName:msgName,
		Payload:string(msgData),
	}

	this.SendChannel <- &message

	return nil
}


func (this *WebSocketRouter) Send(){

	ticker := time.NewTicker(DefaultPingPeriod)

	defer func(){
		ticker.Stop()
		this.Conn.Close()
	}()

	for{
		select {
		case msg, ok := <- this.SendChannel:
			if !ok{
				this.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				common.InfoLog("send channel is closed")
				return
			}

			msgOut, err := json.Marshal(msg)
			if err != nil{
				common.ErrorLog("marsharl failed id:", this.Id, err)
				return
			}

			if err := this.write(websocket.TextMessage, msgOut); err != nil{
				common.ErrorLog("write websocket failed id:", this.Id, err)
				return
			}
		case <-ticker.C:
			if err := this.write(websocket.PingMessage, []byte{}); err != nil{
				return
			}
		}


	}


	for send := range this.SendChannel{

		err := websocket.WriteJSON(this.Conn, send)
		if err != nil{
			common.ErrorLog("ws send failed")
			return
		}
	}
}


func (this *WebSocketRouter) IsClosed() bool{
	select{
	case <-this.SendChannel:
		return true
	default:
		return false
	}
}


func (this *WebSocketRouter) closeUser(){

	this.Conn.Close()
	this.hub.AllWs.Delete(this.Id)
	close(this.SendChannel)

}





























