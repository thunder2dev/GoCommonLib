package bridge

import (
	"MarsXserver/common"
	"container/list"
)

type ObserverType int



type ObserverHandlerFunx func(*ObserverContext)

type ObserverContext struct {
	Data      interface{}
	EventData interface{}
	handler   ObserverHandlerFunx
	forever   bool
}




type ObserverManager struct{

	all	*common.MMap //map[ObserverType]*ObserverContext
	Sid	int

}


func NewObserverManager(sid int) *ObserverManager {

	ob := &ObserverManager{
		all : common.NewMMap(),
		Sid: sid,
	}


	return ob
}


func (this *ObserverManager) RegisterObserver(obType ObserverType, forever_ bool, data_ interface{}, handler_ ObserverHandlerFunx) error{

	if this.all.Get(obType) == nil{
		this.all.Set(obType, list.New())
	}

	li, ok := this.all.Get(obType).(*list.List)
	if ok != true{
		return common.ErrorLog("type:", obType, " is not list")
	}

	li.PushBack(&ObserverContext{
		Data:    data_,
		handler: handler_,
		forever: forever_,
	})

	return nil
}


func (this *ObserverManager) EventHappened(obType ObserverType, eventData interface{}) error{

	common.InfoLog("event happened:", obType)

	dels := make([]*list.Element, 0)

	li, ok := this.all.Get(obType).(*list.List)
	if ok != true{
		common.InfoLog(this.Sid, "observer type:", obType, " not exists")
		return nil
	}


	for e:=li.Front(); e != nil; e = e.Next(){

		ctx, ok := e.Value.(*ObserverContext)
		if ok != true{
			return common.ErrorLog("saved observer is not context")
		}

		ctx.EventData = eventData
		ctx.handler(ctx)

		if ctx.forever == false{
			dels = append(dels, e)
		}
	}

	for _, del := range dels{
		li.Remove(del)
	}


	return nil
}


































