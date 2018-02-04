package bridge

import (
	"reflect"
	"MarsXserver/common"
	"sync/atomic"
)


type PipeRemoteType int

type PipeLineFunc func(req *PipeRequest)error

type PipeLine struct {

	isStart bool

	inout chan *PipeRequest

	handlers map[reflect.Type]PipeLineFunc

	callbacks *common.MMap

	remotes map[PipeRemoteType]*PipeLine

	reqReq	int64
}


type PipeRequest struct{
	seq int64
	data interface{}
}


func NewPipeLine() *PipeLine{

	return &PipeLine{
		inout:     make(chan *PipeRequest),
		handlers:  make(map[reflect.Type]PipeLineFunc),
		callbacks: common.NewMMap(),
		remotes: make(map[PipeRemoteType]*PipeLine),
	}

}


func (this *PipeLine) RegisterRemote(rtype PipeRemoteType, remote *PipeLine){
	this.remotes[rtype] = remote
}

func (this *PipeLine) RegisterHandler(req interface{}, handler PipeLineFunc){

	val := reflect.ValueOf(req)
	if val.Kind() != reflect.Ptr{
		common.ErrorLog("pipe handler req is not ptr")
		return
	}

	tp := reflect.TypeOf(req).Elem()

	this.handlers[tp] = handler

}

func (this *PipeLine) Run(){

	this.isStart = true

	for data := range this.inout {

		dataType := reflect.TypeOf(data.data).Elem()


		handler, ok := this.handlers[dataType]
		if ok != true{
			common.ErrorLog("no pipe handler for", dataType)
			continue
		}

		go handler(data)

	}


	common.InfoLog("pipe line closed")

	this.isStart = false
}


func (this *PipeLine) Send(remoteType PipeRemoteType, data_ interface{}, retCh chan *PipeRequest) error{

	atomic.AddInt64(&this.reqReq, 1)

	this.callbacks.Set(this.reqReq, retCh)

	req := &PipeRequest{
		seq:this.reqReq,
		data:data_,
	}

	remote, ok := this.remotes[remoteType]
	if ok == false{
		return common.ErrorLog("remote type faild", remoteType)
	}

	remote.inout <- req

	return nil
}

func (this *PipeLine) Close(){

	close(this.inout)

}

func (this *PipeLine) IsStart() bool{
	return this.isStart
}
























































