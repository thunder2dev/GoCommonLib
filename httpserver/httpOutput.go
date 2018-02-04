package httpserver

import (
	"encoding/json"
	"MarsXserver/common"
	"io"
	"strings"
	"compress/gzip"
	"compress/flate"
	"strconv"
	"path/filepath"
	"net/http"
	"bytes"
	"fmt"
)

var (
	DefaultCookieNameSanitizer = strings.NewReplacer("\n", "-", "\r", "-")
	DefaultCookieValueSanitizer = strings.NewReplacer("\n", " ", "\r", " ")
)


type HttpOutput struct{


	ctx *HttpContext
	status int
}


type JsonResponse struct{

	Status int
	Msg string
	Body interface{}

}


func NewHttpOutput(_ctx *HttpContext) *HttpOutput{

	return &HttpOutput{
		ctx: _ctx,
	}
}


func (this *HttpOutput) WriteJsonError(err error) error{

	this.SetHeader("Content-Type", "application/json;charset=utf-8")

	jdata, err := json.Marshal(&JsonResponse{
		Status: -1,
		Msg: err.Error(),

	})

	if err != nil{
		common.ErrorLog("marshal json failed")
		return err
	}



	this.WriteBody(jdata)

	return nil

}


func (this *HttpOutput) WriteJson(data interface{}) error{

	this.SetHeader("Content-Type", "application/json;charset=utf-8")

	common.InfoLog("json rsp:", data)

	jdata, err := json.Marshal(&JsonResponse{
		Status: 0,
		Msg: "",
		Body:data,

	})

	if err != nil{
		common.ErrorLog("marshal json failed")
		return err
	}



	this.WriteBody(jdata)

	return nil
}


func (this *HttpOutput) WriteBody(content []byte) error{

	out_writer := this.ctx.Rsp.(io.Writer)

	if this.ctx.Input.GetHeader("Accept-Encoding") != ""{

		encodeings := this.ctx.Input.GetHeader("Accept-Encoding")

		splitted := strings.SplitN(encodeings, ",", -1)

		for _, split := range splitted{

			enc := strings.TrimSpace(split)
			if enc == "gzip"{
				this.SetHeader("Content-Encoding", "gzip")
				out_writer, _ = gzip.NewWriterLevel(this.ctx.Rsp, gzip.BestSpeed)

				break
			}else if enc == "deflate"{
				this.SetHeader("Content-Encoding", "deflate")
				out_writer, _ = flate.NewWriter(this.ctx.Rsp, flate.BestSpeed)
				break
			}
		}
	}else{
		this.SetHeader("Content-Length", strconv.Itoa(len(content)))
	}

	out_writer.Write(content)

	switch writer := out_writer.(type) {
	case *gzip.Writer:
		writer.Close()
		common.InfoLog("write close")
	case *flate.Writer:
		writer.Close()
		common.InfoLog("write close")
	}

	return nil

}

func (this *HttpOutput) SetHeader(key, val string){
	this.ctx.Rsp.Header().Set(key, val)
}

func (this *HttpOutput) AddHeader(key, val string){
	this.ctx.Rsp.Header().Add(key, val)
}


func (this *HttpOutput) Download(filePath string){

	this.SetHeader("Content-Description", "File Transfer")
	this.SetHeader("Content-Type", "application/octet-stream")
	this.SetHeader("Content-Disposition", "attachment; filename=" + filepath.Base(filePath))
	this.SetHeader("Content-Transfer-Encoding", "binary")
	this.SetHeader("Expires", "0")
	this.SetHeader("Cache-Control", "must-revalidate")
	this.SetHeader("Pragma", "public")
	http.ServeFile(this.ctx.Rsp, this.ctx.Req, filePath)

}


func santinizeCookieName(name string) string{

	return DefaultCookieNameSanitizer.Replace(name)
}


func santinizeCookieValue(val string) string{
	return DefaultCookieValueSanitizer.Replace(val)
}

func (this *HttpOutput) WriteCookie(name, value string, args...interface{}){

	var buf bytes.Buffer

	fmt.Fprintf(&buf, "%s=%s", santinizeCookieName(name), santinizeCookieValue(value))

	if len(args) > 0{

		switch arg := args[0].(type) {
		case int:
			if arg > 0{
				fmt.Fprintf(&buf, "; Max-Age=%d", arg)
			}else{
				fmt.Fprintf(&buf, "; Max-Age=0")
			}
		default:
			common.ErrorLog("cookie max age type failed")
			return
		}
	}

	if len(args) > 1{
		if str, ok := args[1].(string); ok == true{
			fmt.Fprintf(&buf, "; Path=%s", santinizeCookieValue(str))
		}else{
			common.ErrorLog("cookie path is not string")
			return
		}
	}


	if len(args) > 2{
		if str, ok := args[1].(string); ok == true{
			fmt.Fprintf(&buf, "; Domain=%s", santinizeCookieValue(str))
		}else{
			common.ErrorLog("cookie path is not string")
			return
		}
	}

	if len(args) > 3{
		fmt.Fprintf(&buf, "; Secure")
	}

	if len(args) > 4{
		fmt.Fprintf(&buf, "; HttpOnly")
	}

	this.ctx.Rsp.Header().Add("Set-Cookie", buf.String())
}


func (this *HttpOutput) SetStatus(status int){
	this.ctx.Rsp.WriteHeader(status)
	this.status = status
}

















