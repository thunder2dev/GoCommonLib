package httpserver


type HttpInput struct{

	ctx *HttpContext
}

func NewHttpInput(_ctx *HttpContext) *HttpInput{

	return &HttpInput{
		ctx: _ctx,
	}
}


func (this *HttpInput) GetHeader(name string) string{

	return this.ctx.Req.Header.Get(name)

}


func (this *HttpInput) Scheme() string{

	if this.ctx.Req.URL.Scheme != ""{
		return this.ctx.Req.URL.Scheme
	}else if this.ctx.Req.TLS == nil{
		return "http"
	}else{
		return "https"
	}
}

func (this *HttpInput) IsWebsocket() bool {
	return this.GetHeader("Upgrade") == "websocket"
}

func (this *HttpInput) IsUpload() bool {

	return this.ctx.Req.MultipartForm != nil

}


func (this *HttpInput) UserAgent() string {

	return this.GetHeader("User-Agent")

}

func (this *HttpInput) IsAjax() bool {
	return this.GetHeader("X-Requested-With") == "XMLhttpRequest"
}

func (this *HttpInput) GetCookie(key string) string{
	ck, err := this.ctx.Req.Cookie(key)
	if err != nil{
		return ""
	}
	return ck.Value
}















