package common

import (
	"container/list"
	"sync"
)

type MList struct{
	list *list.List
	lock sync.RWMutex
}


func NewMList() *MList{

	return &MList{
		list : list.New(),
	}
}


func (this *MList) Length() int{

	this.lock.RLock()
	defer this.lock.RUnlock()

	return this.list.Len()

}

func (this *MList) PushWork(item interface{}){

	this.lock.Lock()
	defer this.lock.Unlock()

	this.list.PushFront(item)
}

func (this *MList) PopWork() interface{}{


	this.lock.Lock()
	defer this.lock.Unlock()

	frontEle := this.list.Front()
	if frontEle == nil{
		return nil
	}

	this.list.Remove(frontEle)

	return frontEle.Value
}


func (this *MList) FrontWork() interface{}{

	this.lock.RLock()
	defer this.lock.RUnlock()

	frontEle := this.list.Front()
	if frontEle == nil{
		return nil
	}

	return frontEle.Value
}






