package common

import (
	"bytes"
	"fmt"
	"strconv"
)


const(
	DEPTH_0_FORMAT = "##########%s##########\n"
	DEPTH_1_FORMAT = "  >>%s<<\n"
	DEPTH_2_FORMAT = "    =>%s\n"
	DEPTH_3_FORMAT = "      ->%s\n"

	DEPTH_MIN = 0
	DEPTH_MAX = 3

)

var (

	formatDic = map[int]string{
		0: DEPTH_0_FORMAT,
		1: DEPTH_1_FORMAT,
		2: DEPTH_2_FORMAT,
		3: DEPTH_3_FORMAT,
	}

)


type InfoWriter struct {

	buff bytes.Buffer

	depth int

}

func NewInfoWriter() *InfoWriter{

	return &InfoWriter{
	}

}


func (this *InfoWriter) Reset(){
	this.buff.Reset()
}

func (this *InfoWriter) String() string {
	return this.buff.String()
}


func (this *InfoWriter) H(depth int) *InfoWriter{

	this.depth = depth

	return this

}

func (this *InfoWriter) Down() *InfoWriter{
	this.depth += 1
	if this.depth > DEPTH_MAX{
		ErrorLog("depth exceeds max")
	}
	return this
}

func (this *InfoWriter) Up() *InfoWriter{
	this.depth -= 1
	if this.depth < DEPTH_MIN{
		ErrorLog("depth below zero")
	}
	return this
}


func (this *InfoWriter) Write(info string) *InfoWriter{
	this.buff.WriteString(fmt.Sprintf(formatDic[this.depth], info))
	return this
}

func (this *InfoWriter) WriteKV(key string, value interface{}) *InfoWriter{

	var valStr string

	switch val := value.(type) {
	case int:
		valStr = strconv.FormatInt(int64(val), 10)
	case int32:
		valStr = strconv.FormatInt(int64(val), 10)
	case int64:
		valStr = strconv.FormatInt(val, 10)
	case float32:
		valStr = strconv.FormatFloat(float64(val), 'f', 6, 64)
	case float64:
		valStr = strconv.FormatFloat(val, 'f', 6, 64)
	case string:
		valStr = val
	default:
		valStr = fmt.Sprintf("%+v", val)
	}

	this.Write(fmt.Sprintf("%s : %s", key, valStr))

	return this

}







































