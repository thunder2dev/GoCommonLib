package common

import "time"

const (
	MaxUint64 = ^uint64(0)
	MinUint64 = 0
	MaxInt64 = int(MaxUint64 >> 1)
	MinInt64 = -MaxInt64 - 1

	Epsilon = 0.0000001


)


var (  //note: must defined per project

	DefaultConfPath = "xxxx"
	DefaultCookieName = "xxxxxxxxxx"
	DefaultProtoBinPath = "xxxx"

)

const (
	DefaultAsyncReadLimit         int           = 10
	DefaultAsyncReadInterval      time.Duration = time.Millisecond * 50
	DefaultRedArrResponseLimit 		int 		= 1000


	Default_Http_Hijack_Max_Wait		time.Duration		= time.Second * 100
	Default_Http_Hijack_Clean_Period	time.Duration		= time.Second * 10
)


const (
	Default_User_Token_Length = 20
)

const(
	SessionKey_UID = "uid"
)

const(
	MessageId_ObjRequestMessageId      = 100
	MessageId_ObjReponseMessageId      = 101

	MessageId_DbObjMessageIdStart      = 200

	MessageId_RedMessageIdStart		   = 300

	MessageId_FileRequest 			   = 400
	MessageId_FileResponse             = 401

	Root_TcpObserverType			   = 500
	Root_HttpObserverType			   = 501
)




var (
	Is_Using_Sys_Time = true
	Is_Using_Time_Elapse = true

	Debug_Time time.Time
	Debug_Time_Start_Time time.Time

)



func SetCurrTimeFromHour(hour, minute, second, milli int){

	now := time.Now().UTC()

	Debug_Time = time.Date(now.Year(), now.Month(), now.Day(), hour, minute, second, milli * 1e6, time.UTC)

	Debug_Time_Start_Time = now

}


func UseDebugTime(useElapse bool){
	Is_Using_Sys_Time = false
	Is_Using_Time_Elapse = useElapse
}


func UseSysTime(){
	Is_Using_Sys_Time = true
}

func GetTimeNow() time.Time{

	if Is_Using_Sys_Time{
		return time.Now().UTC()
	}

	if Is_Using_Time_Elapse{

		passed := time.Now().UTC().UnixNano() - Debug_Time_Start_Time.UnixNano()
		Debug_Time = Debug_Time.Add(time.Nanosecond * time.Duration(passed))

	}

	return Debug_Time

}







