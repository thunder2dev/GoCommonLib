package common

import (
	//"strconv"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const(
	INIT_START_POS = 20
	MSG_MAX_LENGTH = 30960

)




type MBuffer struct {
	buff []byte
	start int
	end int

}

//todo buf size
func NewBuffer() *MBuffer {
	return &MBuffer{buff: make([]byte, MSG_MAX_LENGTH), start: INIT_START_POS, end: INIT_START_POS}
}


func NewBufferByBytes(bts []byte) *MBuffer{

	if len(bts) > MSG_MAX_LENGTH - INIT_START_POS{
		ErrorLog("input byte array too lang")
		return nil
	}

	buf := NewBuffer()

	copy(buf.GetAvailableBuffer(len(bts)), bts)

	buf.SetHaveSupply(len(bts))

	return buf

}

func NewBufferFromBuffer(buf *MBuffer) *MBuffer{

	newBuf := &MBuffer{
		buff: append([]byte(nil), buf.GetDataBuffer()...),
		start: 0,
		end: buf.Length(),
	}

	return newBuf
}


func (this *MBuffer) Info() string{

	return fmt.Sprintf("buf info : start: %d, end: %d, cap:", this.start, this.end, this.Capcity())
}


func (this *MBuffer) Length() int{

	return this.end - this.start

}

func (this *MBuffer) Capcity() int{
	return cap(this.buff) - this.end
}


func (this *MBuffer) SetHaveSupply(n int) error{

	if n > this.Capcity(){
		return errors.New("read exceeds limit")
	}

	this.end += n
	return nil
}

func (this *MBuffer) SetHaveUsed(n int) error{

	if this.start + n > this.end{
		return ErrorLog("used exceeds limit")
	}
	this.start += n
	return nil
}


func (this *MBuffer) GetAvailableBuffer(n int) []byte{

	if this.Capcity() < n{
		ErrorLog("buf cap is insufficient, n:", n, " start:", this.start, " end:", this.end)
		return nil
	}

	return this.buff[this.end:this.end + n]

}

func (this *MBuffer) GetDataBuffer() []byte{

	return this.buff[this.start:this.end]

}



func (this *MBuffer) Append(bytes []byte) error{

	if len(bytes) > this.Capcity(){
		return ErrorLog("buffer cap exceeds")
	}

	length := len(bytes)

	copy(this.buff[this.end: this.end+length], bytes[:])

	this.end += length

	return nil
}

func (this *MBuffer) Prepend(bytes []byte) error{

	if this.start < len(bytes){
		return ErrorLog("buffer head cap exceeds")
	}

	length := len(bytes)

	copy(this.buff[this.start - length: this.start], bytes[:])

	this.start -= length

	return nil

}


func (this *MBuffer) ReadInt() (intVal int, err error){

	if this.start + 4 > this.end{
		return 0, ErrorLog("read int exceeds limit")
	}

	numBytes := this.buff[this.start: this.start + 4]

	var num int32
	reader := bytes.NewReader(numBytes)
	err = binary.Read(reader, binary.BigEndian, &num)

	if err!= nil{
		return 0, ErrorLog("cannot read int")
	}

	this.start += 4


	return int(num), nil
}


func (this *MBuffer) WriteInt(intVal int) error{

	if 4 > this.Capcity(){
		return ErrorLog("write int exceeds limit")
	}

	//numBytes := this.buff[this.end: this.end + 4]

	writer := new(bytes.Buffer)
	err := binary.Write(writer, binary.BigEndian, int32(intVal))

	if err!= nil{
		ErrorLog("cannot read int")
		return err
	}

	copy(this.buff[this.end: this.end + 4], writer.Bytes())

	this.end += 4

	return nil
}

func (this *MBuffer) ReadUint() (uintVal uint32, err error){

	if this.start + 4 > this.end{
		return 0, ErrorLog("read int exceeds limit")
	}

	numBytes := this.buff[this.start: this.start + 4]

	var num uint32
	reader := bytes.NewReader(numBytes)
	err = binary.Read(reader, binary.BigEndian, &num)

	if err!= nil{
		return 0, ErrorLog("cannot read int")
	}

	this.start += 4

	return num, nil
}


func (this *MBuffer) WriteUint(uintVal uint32) error{

	if 4 > this.Capcity(){
		return ErrorLog("write uint exceeds limit")
	}

	//numBytes := this.buff[this.end: this.end + 4]

	writer := new(bytes.Buffer)
	err := binary.Write(writer, binary.BigEndian, uint32(uintVal))

	if err!= nil{
		ErrorLog("cannot read uint")
		return err
	}

	copy(this.buff[this.end: this.end + 4], writer.Bytes())

	this.end += 4

	return nil

}


func (this *MBuffer) ReadLong() (intVal int64, err error){

	if this.start + 8 > this.end{
		return 0, ErrorLog("read int exceeds limit")
	}

	numBytes := this.buff[this.start: this.start + 8]

	var num int64
	reader := bytes.NewReader(numBytes)
	err = binary.Read(reader, binary.BigEndian, &num)

	if err!= nil{
		return 0, ErrorLog("cannot read int")
	}

	this.start += 8

	return num, nil
}


func (this *MBuffer) WriteLong(intVal int64) error{

	if 8 > this.Capcity(){
		return ErrorLog("write long exceeds limit")
	}

	writer := new(bytes.Buffer)
	err := binary.Write(writer, binary.BigEndian, intVal)

	if err!= nil{
		ErrorLog("cannot read int")
		return err
	}

	copy(this.buff[this.end: this.end + 8], writer.Bytes())

	this.end += 8

	return nil
}


func (this *MBuffer) WriteFloat32(fltVal float32) error{

	if 4 > this.Capcity(){
		return ErrorLog("write float64 exceeds limit")
	}

	writer := new(bytes.Buffer)
	err := binary.Write(writer, binary.BigEndian, fltVal)

	if err!= nil{
		ErrorLog("cannot read int")
		return err
	}

	copy(this.buff[this.end: this.end + 8], writer.Bytes())

	this.end += 4

	return nil
}

func (this *MBuffer) ReadFloat32() (fltVal float32, err error){

	if this.start + 4 > this.end{
		return 0, ErrorLog("read float32 exceeds limit")
	}

	numBytes := this.buff[this.start: this.start + 8]

	var num float32
	reader := bytes.NewReader(numBytes)
	err = binary.Read(reader, binary.BigEndian, &num)

	if err!= nil{
		return 0, ErrorLog("cannot read float32")
	}

	this.start += 4

	return num, nil
}




func (this *MBuffer) WriteFloat64(fltVal float64) error{

	if 8 > this.Capcity(){
		return ErrorLog("write float64 exceeds limit")
	}

	writer := new(bytes.Buffer)
	err := binary.Write(writer, binary.BigEndian, fltVal)

	if err!= nil{
		ErrorLog("cannot read int")
		return err
	}

	copy(this.buff[this.end: this.end + 8], writer.Bytes())

	this.end += 8

	return nil
}

func (this *MBuffer) ReadFloat64() (fltVal float64, err error){

	if this.start + 8 > this.end{
		return 0, ErrorLog("read float64 exceeds limit")
	}

	numBytes := this.buff[this.start: this.start + 8]

	var num float64
	reader := bytes.NewReader(numBytes)
	err = binary.Read(reader, binary.BigEndian, &num)

	if err!= nil{
		return 0, ErrorLog("cannot read float64")
	}

	this.start += 8

	return num, nil
}


func (this *MBuffer) ReadByte() (bval byte, err error){

	if this.start + 1 > this.end{
		return 0, ErrorLog("read int exceeds limit")
	}

	bval = this.buff[this.start]

	this.start += 1

	return
}



func (this *MBuffer) WriteByte(bval byte) error{

	if 1 > this.Capcity(){
		return ErrorLog("write byte exceeds limit")
	}

	this.buff[this.end] = bval

	this.end += 1

	return nil
}


func (this *MBuffer) ReadBytes(n int) (res []byte, err error){

	if this.Length() < n {
		return nil, ErrorLog("read buffer exceeds length")
	}

	res = this.buff[this.start: this.start + n]
	this.start += n
	return res, nil
}



func (this *MBuffer) WriteBytes(bts []byte) error {

	if len(bts) > this.Capcity(){
		return ErrorLog("write bytes failed")
	}

	copy(this.buff[this.end: this.end + len(bts)], bts)

	this.end += len(bts)

	return nil

}



func (this *MBuffer) ReadString() (string, error){

	len, err := this.ReadInt()
	if err != nil{
		return "", ErrorLog("string len read failed")
	}

	str := make([]byte, len)
	cplen := copy(str, this.GetDataBuffer())
	if cplen != len{
		return "", ErrorLog("copy str len is mismatched, ori:", len, " cp:", cplen)
	}


	this.SetHaveUsed(len)

	return string(str), nil
}


func (this *MBuffer) WriteString(str string) error {

	len := len(str)

	if this.Capcity() < len + 4{
		return ErrorLog("write string cap is failed")
	}

	if err := this.WriteInt(len); err != nil{
		return ErrorLog("write int failed")
	}

	cplen := copy(this.GetAvailableBuffer(len), []byte(str))
	if cplen != len{
		return ErrorLog("copy len mismatched, ori:", len, " cplen:", cplen)
	}

	this.end += len

	return nil
}



func (this *MBuffer) PrependInt(intVal int) error{

	if this.start < 4{
		return ErrorLog(fmt.Sprintf("buffer head cap exceeds, val:%v", intVal))
	}


	writer := new(bytes.Buffer)
	err := binary.Write(writer, binary.BigEndian, int32(intVal))

	if err!= nil{
		return ErrorLog("cannot read int")
	}

	copy(this.buff[this.start - 4: this.start], writer.Bytes())

	this.start -= 4

	return nil
}

func (this *MBuffer) Reset() {

	this.start = 20
	this.end = 20


}


func (this *MBuffer) Rearrange(){

	len := this.Length()
	if len > 0{
		copy(this.buff[INIT_START_POS:], this.buff[this.start:this.end])
	}
	this.start = INIT_START_POS
	this.end = INIT_START_POS + len
}
















