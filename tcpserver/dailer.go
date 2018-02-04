package tcpserver

import (
	"sync"
	"net"
	"strconv"
	"time"
	"errors"
	"MarsXserver/common"
)

type HashFunc func(*DialerHub, string, int, int, int)(int, int, error)


type DialerHub struct{
	idDic   map[int]*Connector
	typeDic map[string][]int
	hashDic map[string][]HashFunc

	lock sync.RWMutex

	server *TcpServer

	repeatDialerCloseChannel chan struct{}
}


type DialerType string


const (
	dialerTimeOut = time.Second * 50
)




func NewDialerHub(svr *TcpServer) *DialerHub{
	dialerHub := DialerHub{
		idDic:make(map[int]*Connector),
		typeDic:make(map[string][]int),
		hashDic:make(map[string][]HashFunc),
		server:svr,
		}
	return &dialerHub
}


func (this *DialerHub) OnlineNewDialer(dialer *Connector){

	this.lock.Lock()
	defer this.lock.Unlock()

	this.idDic[dialer.sid] = dialer

	if _, ok := this.typeDic[dialer.stype]; ok == false{
		this.typeDic[dialer.stype] = make([]int, 0)
		this.hashDic[dialer.stype] = []HashFunc{DefaultHashFunc}
	}

	this.typeDic[dialer.stype] = append(this.typeDic[dialer.stype], dialer.sid)

}


func (this *DialerHub) OfflineDialer(dialer *Connector){

	this.lock.Lock()
	defer this.lock.Unlock()

	delete(this.idDic, dialer.sid)

	arr := this.typeDic[dialer.stype]

	idx := -1

	for ii, item := range arr{
		if item == dialer.sid{
			idx = ii
			break
		}
	}

	if idx < 0{
		common.ErrorLog("dialer is not existed", dialer.stype, dialer.sid)
	}

	this.typeDic[dialer.stype] = append(arr[:idx], arr[idx+1:]...)

}


func (this *DialerHub) getDialerNumByType(stype string) int{

	arr, ok := this.typeDic[stype]
	if !ok{
		common.ErrorLog("dialer type not existed", stype)
		return 0
	}

	return len(arr)
}

func (this *DialerHub) GetDialerByIdx(stype string, idx int) (*Connector, error){

	this.lock.RLock()
	defer this.lock.RUnlock()

	arr, ok := this.typeDic[stype]
	if !ok{
		return nil, common.ErrorLog("dialer type not existed", stype)

	}

	if idx < 0 || idx >= len(arr){
		return nil, common.ErrorLog("dialer idx exceeded", stype, idx, len(arr))
	}

	sid := arr[idx]

	return this.GetDialerBySid(sid)

}



func DefaultHashFunc(dh *DialerHub, stype string, hashNum, startIdx, endIdx int)(int, int, error){

	cnt := dh.getDialerNumByType(stype)

	if cnt <= 0{
		return -1, -1, common.ErrorLog("dialer cnt 0", stype, cnt)
	}

	ret := (endIdx - startIdx)%cnt + startIdx

	return ret, ret , nil

}


func (this *DialerHub) RegisterDialerHash(stype string, hashFuncs []HashFunc) {

	this.lock.Lock()
	defer this.lock.Unlock()

	if _, ok := this.hashDic[stype]; ok == false{
		common.ErrorLog("dialer type not existed", stype)
		return
	}

	this.hashDic[stype] = hashFuncs

}


func (this *DialerHub) GetAllSidByType(stype string) []int{

	this.lock.RLock()
	defer this.lock.RUnlock()

	return this.typeDic[stype]
}


func (this *DialerHub) GetHashedDialerByType(stype string, hashNums... int)(*Connector, error){

	this.lock.RLock()
	defer this.lock.RUnlock()

	total := this.getDialerNumByType(stype)
	if total <= 0{
		return nil, common.ErrorLog("dialer cnt is less than 0", stype, total)
	}


	funcs, ok := this.hashDic[stype]
	if ok == false{
		return nil, common.ErrorLog("dialer type not existed", stype)
	}

	if len(hashNums) != len(funcs){
		return nil, common.ErrorLog("hash id cnt != hash func", stype, len(hashNums), len(funcs))
	}

	startIdx := 0
	endIdx := total - 1

	for ii, funcItem := range funcs{

		newStartIdx, newEndIdx, err := funcItem(this, stype, hashNums[ii], startIdx, endIdx)
		if err != nil{
			return nil, common.ErrorLog("hash func failed", stype, ii, err)
		}

		startIdx = newStartIdx
		endIdx = newEndIdx
	}

	if startIdx != endIdx{
		return nil, common.ErrorLog("hash func result is not converge", stype, startIdx, endIdx)
	}

	return this.GetDialerByIdx(stype, startIdx)


}



func (this *DialerHub) isDialCanceled() bool{

	this.lock.RLock()
	defer this.lock.RUnlock()

	select{
	case <-this.repeatDialerCloseChannel:
		return true
	default:
		return false
	}

}

func (this *DialerHub) GetDialerBySid(sid int) (*Connector, error) {

	this.lock.RLock()
	defer this.lock.RUnlock()

	dialer, ok := this.idDic[sid]
	if ok == false{
		return nil, common.ErrorLog("dialer is null")
	}

	return dialer, nil
}


func (this *DialerHub) Run(dialers []*Connector) (err error){

	var groupSync sync.WaitGroup

	this.repeatDialerCloseChannel = make(chan struct{})

	okChannel := make(chan int)

	for _, dialer := range dialers{
		groupSync.Add(1)
		go this.repeatDial(dialer, &groupSync, okChannel)
	}

	dialerTimerOutTimer := time.NewTimer(dialerTimeOut)

	go func(){
		groupSync.Wait()
		if common.IsStructClosed(this.repeatDialerCloseChannel) == false{
			close(this.repeatDialerCloseChannel)
		}

		common.InfoLog(this.server.sid, "all dialer wait finished")

		this.server.Observer.EventHappened(ObserverType_DialerFinished, nil)


	}()

okCheckloop:
	for{
		select {
		case okVal := <-okChannel:
			if okVal != 0{
				common.ErrorLog("not ok dial happened", okVal)
				err = errors.New("not ok dial happened")
				close(this.repeatDialerCloseChannel)
			}else{
				common.InfoLog("dial ok val sid:", this.server.sid)
			}
		case <- dialerTimerOutTimer.C:
			common.ErrorLog("time out canceled")
			err = errors.New("time out")
			close(this.repeatDialerCloseChannel)
		case <-this.repeatDialerCloseChannel:
			break okCheckloop
		}	
	}

	common.InfoLog("dialer running is over")

	return

}


func (this *DialerHub) repeatDial(dialer *Connector, groupSync *sync.WaitGroup, okChannel chan<- int){

	//todo check dialer sid is updated

	defer func(){
		common.InfoLog("repeat dial finished")
		if groupSync != nil{
			groupSync.Done()
		}

		if err:=recover(); err != nil{
			common.ErrorLog("repeat dial failed", err)
			okChannel <- dialer.sid
		}
	}()

	for{
		if groupSync != nil{
			if this.isDialCanceled(){
				return
			}
		}

		err := this.dialOne(dialer)
		if err == nil{
			common.InfoLog("dial ok port:", dialer.port)
			break
		}
		time.Sleep(4 * time.Second)
	}

	this.lock.RLock()

	if _, check := this.idDic[dialer.sid]; check == true && groupSync != nil{
		common.ErrorLog("dialer id overlapped, id", dialer.sid)
		okChannel <- dialer.sid
		this.lock.RUnlock()

		return
	}

	this.lock.RUnlock()

	this.OnlineNewDialer(dialer)

	okChannel <- 0

	go ConnectionReadHandler(dialer)
}


func (this *DialerHub) dialOne(dialer *Connector) error{

	//todo timeout

	common.InfoLog("dial:", dialer.ip , ":", dialer.port)

	remoteAddr := dialer.ip + ":" + strconv.Itoa(dialer.port)

	/*
	tcpAddr, err := net.ResolveTCPAddr("tcp4", remoteAddr)
	if err != nil{
		ErrorLog(dialer.ip, dialer.port, err)
		return err
	}*/


	conn, err := net.Dial("tcp", remoteAddr)

	if err != nil{
		common.ErrorLog("sid:", this.server.sid, " connect to ", dialer.ip, dialer.port, " failed", err)
		return err
	}

	dialer.conn = conn


	return nil

}











