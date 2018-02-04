package tcpserver

import (
	"net"
	"sync"
	"github.com/golang/protobuf/proto"
	"errors"
	"time"
	"fmt"
	"MarsXserver/common"
)

const (
	kConnLoopCheckSeconds = 3
)




type Connector struct{
	ip string
	port int

	sid int
	stype string

	uid int64

	conn      net.Conn
	Server    *TcpServer
	writeLock sync.Mutex			//because read is in order, need not lock

	closeChannel chan struct{}
	noRedial	bool
	broadcastChannel chan *proto.Message

}


func (this *Connector) Close(){

	this.conn.Close()
	close(this.closeChannel)
	this.noRedial = true

}




type ConnectorMgr struct{

	server *TcpServer

	connectionSet map[*Connector]struct{}
	uid2connMap   map[int64]*Connector

	closeMapMu sync.RWMutex
	uid2connMu sync.RWMutex

}



func NewConnector(_ip string, _port int, _conn net.Conn,_server *TcpServer) *Connector{

	return &Connector{
		ip:               _ip,
		port:             _port,
		Server:           _server,
		conn:             _conn,
		closeChannel:     make(chan struct{}),
		broadcastChannel: make(chan *proto.Message),
	}

}


func NewDialerConnector(item *TcpServerConfigItem, _server *TcpServer) *Connector{

	return &Connector{
		ip:               item.Ip,
		port:             item.Port,
		sid:              item.Sid,
		stype:		  item.SType,
		Server:           _server,
		closeChannel:     make(chan struct{}),
		broadcastChannel: make(chan *proto.Message),
	}

}

func NewConnectionMgr(_server *TcpServer) *ConnectorMgr{

	return &ConnectorMgr{
		server: _server,
		connectionSet:make(map[*Connector]struct{}),
		uid2connMap:make(map[int64]*Connector),
	}

}


func (cm *ConnectorMgr) AddConn(conn *Connector) error{

	cm.closeMapMu.Lock()
	defer cm.closeMapMu.Unlock()

	if _, ok := cm.connectionSet[conn]; ok == true{
		common.ErrorLog("conn already in close map", conn.ip, conn.port)
		return errors.New("conn already in close map")
	}

	cm.connectionSet[conn] = struct {}{}
	return nil
}





func (cm *ConnectorMgr) Close(conn *Connector) error{

	cm.closeMapMu.RLock()
	defer cm.closeMapMu.RUnlock()

	if _, ok := cm.connectionSet[conn]; ok == false{
		common.ErrorLog("conn not in close map", conn.ip, conn.port)
		return errors.New("conn not in close map")
	}

	close(conn.closeChannel)

	return nil
}

func (cm *ConnectorMgr) RemoveConn(conn *Connector) error{

	cm.closeMapMu.Lock()
	defer cm.closeMapMu.Unlock()

	if _, ok := cm.connectionSet[conn]; ok == false{
		common.ErrorLog("conn not in close map", conn.ip, conn.port)
		return errors.New("conn not in close map")
	}

	close(conn.closeChannel)
	delete(cm.connectionSet, conn)

	return nil
}

func (cm *ConnectorMgr) AddUid2Conn(uid int64, conn *Connector) error{

	cm.uid2connMu.Lock()
	defer cm.uid2connMu.Unlock()

	if _, ok := cm.uid2connMap[uid]; ok == true{
		common.ErrorLog("uid already in uid2conn map", uid, conn.ip, conn.port)
		return errors.New("uid already in uid2conn map")
	}

	if _, ok := cm.connectionSet[conn]; ok == false{
		common.ErrorLog("conn not in close map", conn.ip, conn.port, uid)
		return errors.New("conn not in close map")
	}

	cm.uid2connMap[uid] = conn
	return nil


}


func (cm *ConnectorMgr) RemoveUid2(uid int64, conn *Connector) error{

	cm.uid2connMu.Lock()
	defer cm.uid2connMu.Unlock()

	if _, ok := cm.uid2connMap[uid]; ok == false{
		common.ErrorLog("uid not in uid2conn map", conn.ip, conn.port, uid)
		return errors.New("uid not in uid2conn map")
	}


	delete(cm.uid2connMap, uid)
	cm.Close(conn)
	return nil


}


func (cm *ConnectorMgr) GetConnByUid(uid int64) (conn *Connector, err error){

	cm.uid2connMu.RLock()
	defer cm.uid2connMu.RUnlock()

	if _, ok := cm.uid2connMap[uid]; ok == false{
		common.ErrorLog("uid not in uid2conn map", conn.ip, conn.port, uid)
		err = errors.New("uid not in uid2conn map")
	}

	conn = cm.uid2connMap[uid]

	return
}


func (cm *ConnectorMgr) Run(){
	cm.LoopCheck()
	common.InfoLog("connection manager finished")
}


func (cm *ConnectorMgr) LoopCheckRun(){

	tick := time.Tick(time.Second * kConnLoopCheckSeconds)

cmloopCheck:
	for{
		select {
		case <- cm.server.serverCloseChannel:
			common.InfoLog("cm loop check stopped")
			break cmloopCheck
		case <- tick:
			if err := cm.LoopCheck(); err != nil{
				common.ErrorLog("connecion manager loop check failed")
				cm.server.Close()
			}
		}
	}

}


func (cm *ConnectorMgr) LoopCheck() (err error){

	var connUnreg, connReg int

	for key := range cm.connectionSet {

		if key.uid <= 0{
			connUnreg += 1
			continue
		}

		if _, ok := cm.uid2connMap[key.uid]; ok == false{
			err = errors.New(fmt.Sprintf("reg uid but not in uid2conn map %v", key.uid))
			break
		}
	}

	for key, val := range cm.uid2connMap{

		if _, ok := cm.connectionSet[val]; ok == false{

			common.InfoLog("loop conn in close map:", key)
		}
		connReg += 1
	}

	common.InfoLog("loop check current conn:", connUnreg + connReg, " unreg:", connUnreg, " reg:", connReg)

	return

}











