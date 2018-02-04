package hybridServer

import (
	"reflect"
	"time"
	"MarsXserver/common"
)

type GcInfoTicker struct {

	name string

	gcInfoSelects []reflect.SelectCase
	gcInfoFuncs []func()
	gcInfoNames []string
	minDuration time.Duration
}


func NewGcInfoTicker(name string) *GcInfoTicker{

	return &GcInfoTicker{
		name: name,
		gcInfoNames: make([]string, 0),
		gcInfoSelects: make([]reflect.SelectCase, 0),
		gcInfoFuncs: make([]func(), 0),
	}

}



func (this *GcInfoTicker) AddGcInfo(name string, duration time.Duration, gcFunc func()){

	newTicker := time.NewTicker(duration)

	this.gcInfoSelects = append(this.gcInfoSelects, reflect.SelectCase{Dir:reflect.SelectRecv, Chan: reflect.ValueOf(newTicker.C)})
	this.gcInfoFuncs = append(this.gcInfoFuncs, gcFunc)
	this.gcInfoNames = append(this.gcInfoNames, name)

	if duration < this.minDuration{
		this.minDuration = duration
	}

}

func (this *GcInfoTicker) TickRun() {  //go routine

	common.InfoLog(this.name, "gcinfo tick run ...")

	for{
		chosen, _, ok := reflect.Select(this.gcInfoSelects)
		if ok == false{
			common.ErrorLog("a ticker is closed:", chosen)
		}

		common.InfoLog(this.name + " tick run:" + this.gcInfoNames[chosen])

		this.gcInfoFuncs[chosen]()

	}
}
















