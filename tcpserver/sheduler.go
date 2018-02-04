package tcpserver

import (
	"MarsXserver/common"
)


type EventScheduler struct{

	server *TcpServer

	upChan chan *Connector
	downChan chan *Connector
	bcChan chan BroadcastData

	dailerUpChan chan *Connector
	dailerDownChan chan *Connector

}

func NewEventScheduler(_server *TcpServer) *EventScheduler {
	es := &EventScheduler{}

	es.upChan = make(chan *Connector)
	es.downChan = make(chan *Connector)
	es.bcChan = make(chan BroadcastData)

	es.dailerUpChan = make(chan *Connector)
	es.dailerDownChan = make(chan *Connector)

	es.server = _server

	return es
}


func (es *EventScheduler) Scheduler(){

Schedulerloop:
	for {
		select {

		case bcMsg := <-es.bcChan:
			common.InfoLog("broadcast:", bcMsg.msg)
		case cli:= <-es.upChan:
			common.InfoLog("up local port:", es.server.port, " remote:" , cli.ip , ":" , cli.port)
		case cli:= <-es.downChan:
			common.InfoLog("down local port:", es.server.port, " remote:" , cli.ip , ":" , cli.port)
		case dailer:= <-es.dailerUpChan:
			common.InfoLog("dail successed:", dailer.ip)
		case dailer:= <- es.dailerDownChan:
			common.InfoLog("dail failed:", dailer.ip)
		case <-es.server.serverCloseChannel:
			break Schedulerloop

		}

	}

	common.InfoLog("shedular is stopped")

}