package redmodule

import (
	"github.com/go-redis/redis"
	"MarsXserver/common"
	"time"
)

const(
	Flag_Tm_List_Name   = "tms"
	Default_Gc_Duration = time.Hour * 48
)

type RedSendExprFunc func(req *RedDataRequest, retCh chan interface{})
type RedHandleBytesRspFuncInf func(buf *common.MBuffer, red *XRedis, model *RedHashModel) (*RedDataResponse, error)


type XRedis struct{

	config 		*RedisConfigData
	Conn   		*redis.Client
	IsServer	bool

	sendRedFunc           RedSendExprFunc
	handleRedBytesRspFunc RedHandleBytesRspFuncInf

}

type RedTableType int

const(
	_ RedTableType = iota
	RedTableFlag
	RedTableHash
	RedTableMax
)

type RedValueType int

const (

	_ RedValueType = iota
	RedValueTypeInt
	RedValueTypeInt64
	RedValueTypeString
	RedValueTypeTime
	RedValueTypeMax
)

type RedCountType int

const (
	_ RedCountType = iota
	RedCountTypeByNum
	RedCountTypeBySet

)

type RedModel interface {
	GetTableType() RedTableType
	GetTableName() string
}



type BaseRedModel struct {
}

func GetTableType() RedTableType{
	return RedTableMax
}

func GetTableName() string{
	return ""
}




func NewXRedis(_config *RedisConfigData) *XRedis{

	xredis := &XRedis{
		config:     _config,
	}

	if _config == nil{
		xredis.IsServer = false
	}

	return xredis

}


func (this *XRedis) RegisterSendFunc(sendFunc RedSendExprFunc){
	this.sendRedFunc = sendFunc
}

func (this *XRedis) RegisterBytesRspFunc(rspFunc RedHandleBytesRspFuncInf){
	this.handleRedBytesRspFunc = rspFunc
}

func (this *XRedis) Connect() error{

	this.Conn = redis.NewClient(&redis.Options{
		Addr:		this.config.Addr,
		Password:	this.config.Password,
		DB:		0,
	})

	_, err := this.Conn.Ping().Result()

	if err != nil{
		return common.ErrorLog("connect to redis failed", this.config.Addr, err)
	}

	go this.GC()

	return nil
}






func (this *XRedis) GC(){

	//now := common.GetTimeNow()

	common.InfoLog("redis Session gc is running")

	time.AfterFunc(time.Duration(Default_Gc_Duration), func(){ this.GC()})
}






















