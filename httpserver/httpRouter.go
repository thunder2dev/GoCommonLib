package httpserver

import (
	"net/http"
	"MarsXserver/common"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)



var (
	static_file_types = common.NewMSet(".ico", ".png", ".html", ".css", ".js", ".news", ".jpg", ".unity3d", ".json")
)


type HttpRouterHub struct{

	routes map[string]interface{}
	routesType map[string]reflect.Type

	staticFolderMap map[string]string

	server *HttpServer

}


func NewRouterHub() *HttpRouterHub{

	return &HttpRouterHub{
		routes: make(map[string]interface{}),
		routesType:make(map[string]reflect.Type),
		staticFolderMap: make(map[string]string),
	}

}





func (this *HttpRouterHub) ServeHTTP(w http.ResponseWriter, r *http.Request){

	defer func(){

		if err := recover(); err != nil{
			common.ErrorLog("serve http panic:", r.URL.Path, err)

		}

	}()


	urlPath := r.URL.Path


	if len(urlPath) > 2{

		urlPath = strings.TrimRight(urlPath, "/")

	}

	uri := r.RequestURI

	basePath := filepath.Base(urlPath)
	subs := strings.Split(urlPath, "/")

	if len(subs) < 2{
		common.InfoLog("path illegal:", urlPath)
		NotFound(w, r)
		return
	}


	baseExt := filepath.Ext(basePath)

	if basePath == "favicon.ico"{
		this.SendFile("webapp/view/pics/favicon.ico", w, r)
		return
	}

	common.InfoLog("Req full path:", getIPAdress(r), "path:", urlPath, " dir:", subs, " base:", basePath)

	if len(basePath) > 0 && len(baseExt) > 0 && static_file_types.Contains(baseExt){

		folerPath, ok := this.staticFolderMap[subs[1]]
		if ok == false{
			NotFound(w, r)
			return
		}

		subs[1] = folerPath

		filePath := filepath.Join(subs...)

		common.InfoLog("static file:", filePath)

		this.SendFile(filePath, w, r)

		return
	}


	handlerType, ok := this.routesType[urlPath]
	if !ok{
		common.InfoLog("no handler for path:", urlPath)
		NotFound(w, r)
		return
	}

	if r.Method != "GET" && r.Method != "POST"{
		NotFound(w, r)
		return
	}


	ctx := &HttpContext{
		Rsp:    w,
		Req:    r,
		Server: this.server,
	}

	ctx.Input = NewHttpInput(ctx)
	ctx.Output = NewHttpOutput(ctx)

	handlerInt := reflect.New(handlerType).Interface()

	handler:= handlerInt.(HttpHandler)

	handler.InitBaseHandler(this.server, ctx)

	//if ctx.Input.IsWebsocket() == false{
		handler.StartSession()
	//}

	if this.server.AuthFunc != nil{

		if handler.NeedAuth(){

			authOk, err := this.server.AuthFunc(handler, ctx)
			if err != nil{
				ctx.Output.WriteJsonError(err)
				return
			}
			if !authOk{
				ctx.Output.WriteJsonError(common.ErrorLog("auth"))
				return
			}

		}
	}

	if r.Method == "GET"{
		handler.Get(ctx)
	}else if r.Method == "POST"{
		handler.Post(ctx)
	}

	if this.server.templateManager != nil{

		handler.Render()

	}


	//subUrls := strings.Split(urlPath, "/")

	common.InfoLog(urlPath, uri)

}



func (this *HttpRouterHub) RegisterStaticFolder(url string, path string) error{

	if _, ok := this.staticFolderMap[url]; ok == true{
		return common.ErrorLog("url is already registered")
	}

	this.staticFolderMap[url] = path

	return nil
}


func (this * HttpRouterHub) ShowHomePage(){
	common.InfoLog("showing home page")
}



func (this * HttpRouterHub) SendFile(filePath string, w http.ResponseWriter, r *http.Request){

	absPath, err := filepath.Abs(filePath)
	if err != nil{
		NotFound(w, r)
		common.ErrorLog("file path error", filePath)
		return
	}



	_, err = os.Stat(absPath)
	if err != nil{
		NotFound(w, r)
		common.ErrorLog("not found file", absPath)
		return
	}

	http.ServeFile(w, r, filePath)

}




func (this *HttpRouterHub) RegisterRouter(path string, handler interface{}){

	this.routes[path] = handler

	dataType := reflect.TypeOf(handler)

	this.routesType[path] = dataType.Elem()

}



func (this *HttpRouterHub) WebsocketRoute(ctx *HttpContext, path string, body string){
	handlerType, ok := this.routesType[path]
	if !ok{
		common.ErrorLog("no handler for path:", path)
		return
	}
	handlerInt := reflect.New(handlerType).Interface()

	handler:= handlerInt.(HttpHandler)

	ctx.WsBody = body

	handler.Ws(ctx)


}









































