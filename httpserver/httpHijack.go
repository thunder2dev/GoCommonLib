package httpserver

import (
	"net/http"
	"MarsXserver/common"
	"net"
	"bufio"
	"sync"
	"time"
)



type HijackCleanNotifyFunc func(ctx *HijackCtx)


type HijackCtx struct{

	//UId			int64
	Conn		net.Conn
	TimeStamp   time.Time
	IsClosed	bool

	RW			*bufio.ReadWriter
}



type HijackHub struct{

	Ctxs	[]*HijackCtx

	cleanNotifyFuncs	[]HijackCleanNotifyFunc

	lock	sync.Mutex

}


func NewHijackHub() *HijackHub{

	newHJ := &HijackHub{
		Ctxs: make([]*HijackCtx, 0),
		cleanNotifyFuncs: make([]HijackCleanNotifyFunc, 0),
	}

	go newHJ.Run()

	return newHJ
}


func (this *HijackHub) RegisterCleanNotify(notifyFunc HijackCleanNotifyFunc){

	this.cleanNotifyFuncs = append(this.cleanNotifyFuncs, notifyFunc)


}

func (this *HijackHub) NewHijack(rw http.ResponseWriter) (*HijackCtx, error){

	this.lock.Lock()
	defer this.lock.Unlock()

	hj, ok := rw.(http.Hijacker)
	if !ok{
		return nil, common.ErrorLog("http response writer is not hijack")
	}

	conn, buffrw, err := hj.Hijack()

	if err != nil{
		return nil, common.ErrorLog("hijack failed", err)
	}

	newHj := &HijackCtx{
		Conn:	conn,
		RW:		buffrw,
		IsClosed: false,
		TimeStamp: common.GetTimeNow(),
	}

	this.Ctxs = append(this.Ctxs, newHj)

	return newHj, nil
}




func (this *HijackHub) Run(){

	ticker := time.NewTicker(common.Default_Http_Hijack_Clean_Period)

	defer func(){
		ticker.Stop()
	}()

	for{
		select {
		case <-ticker.C:
			this.Clean()
		}
	}
}


func (this *HijackHub) Clean(){

	this.lock.Lock()
	defer this.lock.Unlock()

	newCtxArr := make([]*HijackCtx, 0, len(this.Ctxs))
	now := common.GetTimeNow()

	for _, ctx := range this.Ctxs{

		if now.Unix() - ctx.TimeStamp.Unix() > int64(common.Default_Http_Hijack_Max_Wait/time.Second){
			ctx.Conn.Close()
			ctx.RW.Write([]byte("{ \"error\":\"timeout\")"))
			ctx.IsClosed = true

			for _, funcItem := range this.cleanNotifyFuncs{
				funcItem(ctx)
			}

		}else{
			newCtxArr = append(newCtxArr, ctx)
		}
	}

	this.Ctxs = newCtxArr
}


func (this *HijackCtx) Close(){

	this.Conn.Close()

}









