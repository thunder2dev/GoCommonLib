package common

import (
	"runtime"
	"fmt"
	"reflect"
	"os"
	"time"
	"io"
	mrand "math/rand"

	"crypto/sha1"
	"crypto/rand"
	"encoding/hex"

)

func GetErrorStack() string{

	stack := "----------------------------------------------\n"

	for i:=1; ; i++{
		_, file, line, ok := runtime.Caller(i)
		if !ok{
			break
		}
		stack += fmt.Sprintln(file, line)
	}

	stack += "----------------------------------------------"

	return stack
}


func IsInterfaceChannelClosed(ch chan interface{}) bool {

	var ok bool

	select {
	case _, ok = <-ch:
	default:
		ok = true
	}

	return !ok

}

func IsStructClosed(ch chan struct{}) bool {

	var ok bool

	select {
	case _, ok = <-ch:
	default:
		ok = true
	}

	return !ok

}


func IsDefaultValueOfType(x interface{}) bool{

	return x == reflect.Zero(reflect.TypeOf(x)).Interface()

}



func IsFileExists(file string) bool {

	_, err := os.Stat(file)
	if err != nil{
		return false
	}

	return true

}




func GetFieldValueByInterface(obj interface{}, fname string) (val interface{}, err error){

	objVal := reflect.ValueOf(obj)

	ind := reflect.Indirect(objVal)

	field := ind.FieldByName(fname)

	if !field.IsValid(){
		return nil, ErrorLog("no field")
	}

	return field.Interface(), nil
}

func GetFieldValueByRValue(objVal reflect.Value, fname string) (val interface{}, err error){

	ind := reflect.Indirect(objVal)

	field := ind.FieldByName(fname)

	if !field.IsValid(){
		return nil, ErrorLog("no field")
	}

	return field.Interface(), nil
}


func PrintTime(tm time.Time) string{

	return tm.Format("2006-01-02 15:04:05")
}

func MakeDBTimeString(tm time.Time) string{

	return tm.Format("2006-01-02 15:04:05")

}

func FromDBTimeString(tmStr string) (time.Time, error){

	tm, err := time.Parse("2006-01-02 15:04:05", tmStr)
	if err != nil{
		return tm, err
	}

	return tm.UTC(), nil

}


func SafeCloseArrChannel(chs chan []interface{}){

	defer func(){

		if err := recover(); err != nil{
			ErrorLog("closed a closed pipe", err)
		}
	}()

	close(chs)
}


func SafeCloseChannel(chs chan interface{}){

	defer func(){

		if err := recover(); err != nil{
			ErrorLog("closed a closed pipe", err)
		}
	}()

	close(chs)
}

func ShuffleIntSlice(src []int) []int{

	dst := make([]int, len(src))
	perm := mrand.Perm(len(src))
	for ii, vv := range perm{
		dst[vv] = src[ii]
	}

	return dst

}

func GenerateRandString(count int) string{

	randBytes := make([]byte, count)
	io.ReadFull(rand.Reader, randBytes)

	sKey := fmt.Sprintf("%s%d", randBytes, GetTimeNow().Nanosecond())

	sha1 := sha1.New()
	sha1.Write([]byte(sKey))
	return hex.EncodeToString(sha1.Sum(nil))
}


func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}



func WhereAmI(depthList ...int) string {
	var depth int
	if depthList == nil {
		depth = 1
	} else {
		depth = depthList[0]
	}
	function, file, line, _ := runtime.Caller(depth)
	return fmt.Sprintf("File: %s  Function: %s Line: %d", file, runtime.FuncForPC(function).Name(), line)
}




func GetHashNumByString(str string) uint32{

	if len(str) <= 0{
		return 0
	}

	arr := []byte(str)

	if len(arr)%2 == 1{
		arr = append(arr, 0)
	}

	halfLen := uint32(len(arr)/2)
	ret := uint32(0)

	for ii:=uint32(0); ii < halfLen; ii++{
		jj := ii * 2
		offset := ii&0x0f
		ret = ret ^ ( (uint32(arr[jj]) << (offset + 8)) | (uint32(arr[jj+1]) << offset))
	}

	return ret

}



























