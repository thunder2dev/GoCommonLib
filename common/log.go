package common

import (
	"log"
	"os"
	"errors"
	"fmt"
	"bytes"
	"reflect"
)

const (
	LogLevelTrace = iota
	LogLevelDebug
	LogLevelInfo
	LogLevelWarning
	LogLevelError
	LogLevelCritical
)


var logLevel = LogLevelTrace

func LogLevel() int{
	return logLevel
}

func SetLogLevel(level int){
	logLevel = level
}

var mlogger = log.New(os.Stdout, "", log.Ldate|log.Ltime)
var mloggerNoDate = log.New(os.Stdout, "", log.LstdFlags)



func MSprintfOne(v reflect.Value) string {

	ind := reflect.Indirect(v)

	typeData := ind.Type()
	//InfoLog(typeData.Name())

	buff := new(bytes.Buffer)

	switch ind.Kind() {

	case reflect.Slice:
		switch ind.Type().Elem().Kind() {
		default:
			buff.WriteString("[")
			for ii := 0; ii < ind.Len(); ii++ {
				buff.WriteString(MSprintfOne(ind.Index(ii)))
				buff.WriteString(",")
			}
			buff.WriteString("]")
		}

	case reflect.Map:
		keys := ind.MapKeys()
		for _, key := range keys{
			buff.WriteString("(")

			keystr := MSprintfOne(key)
			buff.WriteString(keystr)
			buff.WriteString(":")

			mVal := ind.MapIndex(key)
			mValStr := MSprintfOne(mVal)

			buff.WriteString(mValStr)

			buff.WriteString(",")
		}
		buff.WriteString(")")

	case reflect.Ptr:
		//ind2 := reflect.Indirect(ind)
		buff.WriteString(MSprintfOne(ind.Elem()))

	case reflect.Struct:
		len := ind.NumField()

		buff.WriteString("{")

		for ii := 0; ii < len; ii++{

			field := ind.Field(ii)

			//InfoLog(typeData.Field(ii).Name + " ")

			buff.WriteString(typeData.Field(ii).Name)

			buff.WriteString(":")

			buff.WriteString(MSprintfOne(field))

			buff.WriteString(",")
		}

		buff.WriteString("}")
	case reflect.Bool:

		buff.WriteString(fmt.Sprintf("%+v", ind.Bool()))

	case reflect.String:

		buff.WriteString(ind.String())

	case reflect.Int, reflect.Int32, reflect.Int64:
		buff.WriteString(fmt.Sprintf("%+v", ind.Int()))

	case reflect.Float32, reflect.Float64:
		buff.WriteString(fmt.Sprintf("%+v", ind.Float()))

	case reflect.Interface:
		buff.WriteString(MSprintfOne(ind.Elem()))

	default:
		buff.WriteString(fmt.Sprintf("no value type"))
	}

	return buff.String()

}

func MSprintf(vs ...interface{}) string{

	buff := new(bytes.Buffer)

	for _, v := range vs{

		oneStr := MSprintfOne(reflect.ValueOf(v))

		buff.WriteString(oneStr)
		buff.WriteString(" ")

	}

	return buff.String()

}


func DebugLog(v ...interface{}){
	if LogLevelInfo >= logLevel{
		mlogger.Printf("[Debug] %v\n", v)
	}
}

func DebugLogPlus(v ...interface{}){
	if LogLevelInfo >= logLevel{
		resStr := MSprintf(v...)
		mlogger.Printf("[Info] %v\n", resStr)
	}
}

func InfoLog(v ...interface{}){
	if LogLevelInfo >= logLevel{
		mlogger.Printf("[Info] %+v\n", v)
	}
}


func WarnLog(v ...interface{}){
	if LogLevelWarning >= logLevel{
		mlogger.Printf("[Warning] %+v\n", v)
	}
}


func ErrorLog(v ...interface{}) error{
	var err error
	if LogLevelError >= logLevel{
		errStr := fmt.Sprintf("[Error] %v\n", v)

		err = errors.New(errStr)

		mlogger.Printf(errStr)
	}
	mloggerNoDate.Printf("%v", GetErrorStack())

	return err
}

func FatalLog(v ...interface{}){
	if LogLevelError >= logLevel{
		mlogger.Print("[Fatal] %v\n", v)
	}
	mloggerNoDate.Printf("%v", GetErrorStack())

	os.Exit(-1)

}





