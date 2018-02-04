package common

import (
	"time"
)

type ObjMessageId int


const (
	_ ObjMessageId = MessageId_ObjReponseMessageId + iota
	MessageId_HeartBeatRequest
	MessageId_HeartBeatResponse
	MessageId_ConnectRequest
	MessageId_ConnectResponse

)



type ObjRequestHeader struct {
	Seq 	int64
	Id 	int
}

type ObjResponseHeader struct {
	Seq int64
	Err int
	Id   int

}

type HeartBeatRequest struct{
	time time.Time
}

type HeartBeatResponse struct {
	duration time.Duration
}

type ConnectRequest struct{
}

type ConnectResponse struct {
}