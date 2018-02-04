package httpserver

import (
	"MarsXserver/common"
	"net/url"
	"strings"
	"reflect"
	"strconv"
	"net/http"
	"io/ioutil"
	"os"
)

const (
	DefaultParseFormMem = 1024*1024
)

type HttpHandler interface{

	Prepare(context *HttpContext)
	Post(context *HttpContext)
	Get(context *HttpContext)
	Ws(context *HttpContext)

	InitBaseHandler(server *HttpServer, ct *HttpContext)

	NeedAuth() bool

	StartSession()
	SetSession(name string, val interface{})
	GetSessinon(name string) interface{}
 	DelSession(name string)

	Render() error
}




type BaseHttpHandler struct{

	server *HttpServer

	ctx *HttpContext

	handlerType reflect.Type

	needRender bool

	tpl string

	tplData interface{}

}


func (this *BaseHttpHandler) NeedRender(tpl string, data interface{}){

	this.needRender = true

	this.tpl = tpl

	this.tplData = data


}


func (this *BaseHttpHandler) InitBaseHandler(server *HttpServer, ctx *HttpContext){

	this.server = server
	this.ctx = ctx

}


func (this *BaseHttpHandler) NeedAuth() bool{

	return true

}


func (this *BaseHttpHandler) Get(context *HttpContext){
	http.Error(context.Rsp, "Get Method Not Allowed", 405)
}

func (this *BaseHttpHandler) Post(context *HttpContext){
	http.Error(context.Rsp, "Post Method Not Allowed", 405)
}

func (this *BaseHttpHandler) Prepare(context *HttpContext){

	common.InfoLog("prepare handler:", context.Req.RequestURI)

}

func (this *BaseHttpHandler) Ws(context *HttpContext){

	common.ErrorLog("no ws handler")

}


func (this *BaseHttpHandler) Render() error{

	if this.needRender == false{
		return nil
	}

	tpl, err := this.server.templateManager.GetTemplate(this.tpl)

	if err != nil{
		return err
	}

	tpl.Execute(this.ctx.Rsp, this.tplData)

	return nil
}


func (this *BaseHttpHandler) ParseForm(obj interface{}) (form url.Values, err error){

	formVals, err := this.GetForm()
	if err != nil{
		return nil, common.ErrorLog("get form vals failed")
	}

	oriV := reflect.ValueOf(obj)

	objV := reflect.Indirect(oriV)
	objT := objV.Type()

	if oriV.Kind() != reflect.Ptr || oriV.Elem().Kind() != reflect.Struct{
		return nil, common.ErrorLog("input para must be struct ptr")
	}

	for ii := 0; ii < objV.NumField(); ii++{

		fieldV := objV.Field(ii)
		fieldT := objT.Field(ii)

		if fieldV.CanSet() == false{
			continue
		}

		fname := fieldT.Name
		common.InfoLog("parse form :", fname)

		tag := fieldT.Tag.Get("form")

		value := formVals.Get(fname)

		switch tag {
		case "empty":
			if len(value) <= 0{
				return nil, common.ErrorLog("param :", fname, " is empty.")
			}
		}

		if len(value) <= 0{
			continue
		}

		switch fieldV.Kind(){
		case reflect.Bool:
			b, err := strconv.ParseBool(fname)
			if err != nil{
				continue
			}
			fieldV.SetBool(b)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			ival, err := strconv.ParseInt(value, 10, 64)
			if err != nil{
				continue
			}
			fieldV.SetInt(ival)
		case reflect.Float32, reflect.Float64:
			ival, err := strconv.ParseFloat(value, 64)
			if err != nil{
				continue
			}
			fieldV.SetFloat(ival)
		case reflect.String:
			fieldV.SetString(value)
		}
	}

	return formVals, nil
}




func (this *BaseHttpHandler) GetForm() (form url.Values, err error){

	contentType := this.ctx.Req.Header.Get("Content-Type")

	if strings.Contains(contentType, "multipart/form-data"){
		err = this.ctx.Req.ParseMultipartForm(DefaultParseFormMem)
	}else {
		err = this.ctx.Req.ParseForm()
	}

	if err != nil{
		return nil, err
	}

	return this.ctx.Req.Form, nil
}


// 获取文件大小的接口
type Size interface {
	Size() int64
}

// 获取文件信息的接口
type Stat interface {
	Stat() (os.FileInfo, error)
}



func (this *BaseHttpHandler) GetFormFileBytes(formKey string)(string, []byte, error){   //must parseform before this func

	file, handler, err := this.ctx.Req.FormFile(formKey)
	switch err {
	case nil:
	case http.ErrMissingFile:
		return "", nil, new(common.DefaultEmptyError)
	default:
		return "", nil, new(common.DefaultEmptyError)
	}

	fileHeader := make([]byte, 512)

	if _, err := file.Read(fileHeader); err != nil{
		return "", nil, common.ErrorLog("read file header failed", " err:", err)
	}

	defer file.Close()

	fileMime := http.DetectContentType(fileHeader)
	common.InfoLog("pic file name:", handler.Filename, " type:", fileMime)

	if _, err := file.Seek(0, 0); err != nil{
		return "", nil, common.ErrorLog("seek file failed", err)
	}


	if statInterface, ok := file.(Stat); ok {
		fileInfo, _ := statInterface.Stat()
		common.InfoLog( "上传文件的大小为: %d", fileInfo.Size())
	}
	if sizeInterface, ok := file.(Size); ok {
		common.InfoLog( "上传文件的大小为: %d", sizeInterface.Size())
	}

	fileData, err := ioutil.ReadAll(file)
	if err != nil{
		return "", nil, common.ErrorLog(" read uploadpic file data failed")
	}

	return handler.Filename, fileData, nil

}



func (this *BaseHttpHandler) StartSession(){

	this.ctx.Session = this.server.sessionManager.SessionStart(this.ctx.Rsp, this.ctx.Req)

}


func (this *BaseHttpHandler) SetSession(name string, val interface{}){

	this.ctx.Session.Set(name, val)
}

func (this *BaseHttpHandler) GetSessinon(name string) interface{}{

	return this.ctx.Session.Get(name)

}

func (this *BaseHttpHandler) DelSession(name string){

	this.ctx.Session.Delete(name)

}


















