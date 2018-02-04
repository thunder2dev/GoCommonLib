package tcpserver

import "net"


type Controller struct{

	conn net.Conn
	reqMsg interface{}

}
