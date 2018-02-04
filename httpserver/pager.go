package httpserver

import (
	"encoding/json"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"net/http"
	"golang.org/x/net/html"
	"MarsXserver/common"
	"bytes"
	"net/url"
	"net/http/cookiejar"
	"strconv"
	"time"
	"io/ioutil"
)

const (
	Rule_Value_Type_Attr = 1
	Rule_Value_Type_Text = 2
	Rule_Value_Type_Html = 3
	Rule_Value_Type_MultiText = 4


	Test_For_Each_Count = 4

	Http_Get_TimeOut = time.Second * 4
)

type RuleObject struct {

	Key string
	ValType int
	Val string

}

func NewRuleObject(jstr string) *RuleObject{

	ruleObj := new(RuleObject)
	json.Unmarshal([]byte(jstr), ruleObj)
	return ruleObj
}

func (this *RuleObject) String() string{

	return fmt.Sprintf("key:%s, valType:%d, val:%s", this.Key, this.ValType, this.Val)

}

type NodeHandler func(rule *RuleObject, ret interface{}) error


const(
	Node_Type_Enter = 1
	Node_Type_Get = 2
	Node_Type_Each = 3
	Node_Type_Html = 4
)

type AnalyzerNode struct {

	name 	string
	nodeType int
	rule	*RuleObject
	handler NodeHandler

}



type HtmlPageAnalyzer struct {

	startUrl string
	nodes []*AnalyzerNode

	cookieJar *cookiejar.Jar


	res *HtmlPageResult


	skipErr bool
	errs []error
	info *common.Stack

	isTest bool
	eachWaitDuration time.Duration
}



type HtmlPageResult struct{

	UrlRef *url.URL

	Name string

	html string

	Vals map[string]string

	childMap map[string]*HtmlPageResult

	childArr []*HtmlPageResult


	info *common.Stack

	Err error

}


func (this *HtmlPageResult) At(name string) *HtmlPageResult{

	if this == nil{
		common.ErrorLog("")
		return this
	}

	if this.Err != nil{
		return this
	}

	this.info.Push(name)
	defer this.info.Pop()

	if this.Name == name{
		return this
	}

	if this.childMap == nil{
		this.Err = common.ErrorLog("child map null", PrintInfoStack(this.info))
		return this
	}

	child, ok := this.childMap[name]
	if !ok{
		this.Err = common.ErrorLog("node is not found", PrintInfoStack(this.info))
		return this
	}

	if child == nil{
		this.Err = common.ErrorLog("node is null", PrintInfoStack(this.info))
		return this
	}

	child.info = this.info
	child.Err = this.Err
	return child

}

func (this *HtmlPageResult) Len() int{

	if this.Err != nil{
		return 0
	}


	if this.childArr == nil{
		this.Err = common.ErrorLog("node arr nil", PrintInfoStack(this.info))
		return 0
	}

	return len(this.childArr)
}


func (this *HtmlPageResult) Idx(index int) *HtmlPageResult{

	if this.Err != nil{
		return this
	}

	if this.childArr == nil || index >= len(this.childArr){
		this.Err = common.ErrorLog("node arr Err", len(this.childArr), " idx:", index, PrintInfoStack(this.info))
		return this
	}


	this.childArr[index].info = this.info
	this.info.Push(strconv.Itoa(index))

	return this.childArr[index]
}


func (this *HtmlPageResult) Get(name string) (string, error){
	if this.Err != nil{
		return "", this.Err
	}

	if this.Vals == nil{
		return "", common.ErrorLog("node val nil", name, PrintInfoStack(this.info))
	}

	res, ok := this.Vals[name]
	if ok != true{
		return "", common.ErrorLog("attr is not exists", name, PrintInfoStack(this.info))
	}

	return res, nil

}

func (this *HtmlPageResult) Html() (string, error){

	if this.Err != nil{
		return "", this.Err
	}

	return this.html, nil
}



func PrintInfoStack(info *common.Stack) string{

	infoRes := make([]string, info.Len())

	info.ForEach(func(val interface{}){
		infoRes = append(infoRes, val.(string))
	})

	var printBuffer bytes.Buffer

	for ii := len(infoRes) -1; ii >= 0; ii--{

		printBuffer.WriteString(fmt.Sprintf("%s ", infoRes[ii]))
	}

	return printBuffer.String()

}





func NewHtmlPageAnalyzer(rootUrl string, skipError bool ) *HtmlPageAnalyzer{

	aly := &HtmlPageAnalyzer{
		startUrl: rootUrl,
		nodes: make([]*AnalyzerNode, 0),
		res: new(HtmlPageResult),
		skipErr: skipError,
		errs: make([]error, 0),
		info: new(common.Stack),

	}


	aly.cookieJar, _ = cookiejar.New(nil)

	return aly

}

func (this *HtmlPageAnalyzer)AddNode(nodeName string, nodeType int, rule *RuleObject, handler NodeHandler){

	this.nodes = append(this.nodes, &AnalyzerNode{nodeName, nodeType, rule,handler})
}

func (this *HtmlPageAnalyzer) SetTest(eachWaitDuration time.Duration) *HtmlPageAnalyzer{

	this.isTest = true
	this.eachWaitDuration = eachWaitDuration
	return this

}

func (this *HtmlPageAnalyzer)ForEach(name string, rule *RuleObject) *HtmlPageAnalyzer{

	this.AddNode(name, Node_Type_Each, rule, nil)
	return this
}


func (this *HtmlPageAnalyzer)Enter(name string, rule *RuleObject) *HtmlPageAnalyzer{

	this.AddNode(name, Node_Type_Enter, rule, nil)
	return this
}

func (this *HtmlPageAnalyzer)Get(name string, rule *RuleObject) *HtmlPageAnalyzer{
	this.AddNode(name, Node_Type_Get, rule, nil)
	return this
}

func (this *HtmlPageAnalyzer)GetHtml() *HtmlPageAnalyzer{
	this.AddNode("", Node_Type_Html , &RuleObject{"", Rule_Value_Type_Html, ""}, nil)
	return this
}

func (this *HtmlPageAnalyzer)Run() (*HtmlPageResult, error){

	sel, err := this.start()
	if err != nil{
		return nil, err
	}

	urlRef, err := url.Parse(this.startUrl)
	if err != nil{
		return nil, err
	}

	this.res.UrlRef = urlRef
	this.res.info = new(common.Stack)

	this.handleNode(sel, 0, this.res)



	if len(this.errs) > 0{
		var errBuff bytes.Buffer

		for _, err := range this.errs{

			errBuff.WriteString(err.Error() + "\n")
		}

		return this.res, common.ErrorLog(errBuff.String())
	}


	return this.res, nil

}


func (this *HtmlPageAnalyzer) handleNode(curr *goquery.Selection, nodeIndex int, res *HtmlPageResult){

	if len(this.errs) > 0 && this.skipErr == false{
		return
	}

	if nodeIndex >= len(this.nodes){
		return
	}

	node := this.nodes[nodeIndex]

	switch node.nodeType {
	case Node_Type_Each:
		this.executeEach(curr, nodeIndex, res)
		break
	case Node_Type_Get:
		this.executeGet(curr, nodeIndex, res)
		break
	case Node_Type_Enter:
		this.executeEnter(curr, nodeIndex, res)
		break
	case Node_Type_Html:
		this.executeHtml(curr, nodeIndex, res)
		break
	}


}


func (this *HtmlPageAnalyzer) start() (*goquery.Selection, error){

	rsp, err := http.Get(this.startUrl)
	if err != nil || rsp == nil || rsp.StatusCode < 200 || rsp.StatusCode >= 300 {
		return nil, common.ErrorLog("start client connect failed, addr:", this.startUrl, err)
	}
	common.InfoLog("get doc from ", this.startUrl)


	defer rsp.Body.Close()

	var doc *goquery.Document

	node, err := html.Parse(rsp.Body)
	if err != nil{
		return nil, common.ErrorLog("html parse failed, addr:", err)
	}else{
		doc = goquery.NewDocumentFromNode(node)
	}

	return doc.Selection, nil
}




func (this *HtmlPageAnalyzer) executeEach(curr *goquery.Selection, nodeIndex int, res *HtmlPageResult){

	if len(this.errs) > 0 && this.skipErr == false{
		return
	}

	node := this.nodes[nodeIndex]

	this.info.Push(node.name)
	defer this.info.Pop()

	sel := curr.Find(node.rule.Key)

	length := sel.Length()

	res.Name = node.name
	res.childArr = make([]*HtmlPageResult, length)
	sel.Each(func(index int, s *goquery.Selection){

		if this.isTest && index > Test_For_Each_Count{
			return
		}

		if len(this.errs) > 0 && this.skipErr == false{
			return
		}

		this.info.Push(fmt.Sprintf("[%d]", index))
		defer this.info.Pop()

		res.childArr[index] = new(HtmlPageResult)
		res.childArr[index].UrlRef = res.UrlRef

		this.handleNode(s, nodeIndex+1, res.childArr[index])

		if this.isTest{

			time.Sleep(this.eachWaitDuration)

		}
	})

	return

}

func (this *HtmlPageAnalyzer) executeEnter(curr *goquery.Selection, nodeIndex int, res *HtmlPageResult){

	if len(this.errs) > 0 && this.skipErr == false{
		return
	}

	node := this.nodes[nodeIndex]

	this.info.Push(node.name)
	defer this.info.Pop()


	var outErr error
	var outputAsBytes []byte

	var doc *goquery.Document
	newRes := new(HtmlPageResult)

	for{
		subUrl, err := GetItemFromSelection(node.rule, curr)
		if err != nil {
			outErr = err
			break
		}

		parsedUrl, err := res.UrlRef.Parse(subUrl)
		if err != nil{
			outErr = common.ErrorLog("parse url failed:", subUrl, " when getting list of ", res.UrlRef.String(), err)
			break
		}

		_url := res.UrlRef.ResolveReference(parsedUrl)

		common.InfoLog("pager enter:", _url.String())

		//client := http.Client{Timeout: Http_Get_TimeOut, Jar: this.cookieJar}

		/*transport := &MTransport{
			ConnectTimeout:        20*time.Second,
			RequestTimeout:        20*time.Second,
			ResponseHeaderTimeout: 20*time.Second,
		}
		defer transport.Close()*/

//		client := &http.Client{MTransport: transport, Jar:this.cookieJar}

		req, _ := http.NewRequest("Get", _url.String(), nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36")
		req.Header.Set("Referer", "http://toyokeizai.net/list/genre/business")

//		rsp, err := client.Do(req)

		rsp, err := http.Get(_url.String())
		if err != nil || rsp == nil{
			outErr = common.ErrorLog("enter client connect failed, addr:", _url, err)
			break
		}

		if rsp.StatusCode < 200 || rsp.StatusCode >= 300{

			outputAsBytes, err = ioutil.ReadAll(rsp.Body)
			rsp.Body.Close()

			common.InfoLog("status error:", string(outputAsBytes))
			outErr = common.ErrorLog("enter client status code wrong", rsp.StatusCode)
			break

		}



		defer rsp.Body.Close()

		docNode, err := html.Parse(rsp.Body)
		if err != nil{
			outErr = common.ErrorLog("html parse failed, addr:", err)
			break
		}else{
			doc = goquery.NewDocumentFromNode(docNode)
		}

		common.DebugLogPlus("enter finished", doc.Text()[0:50])

		newRes.UrlRef = _url

		break
	}


	if outErr != nil{

		newErr := common.ErrorLog(PrintInfoStack(this.info), outErr)
		this.errs = append(this.errs, newErr)
		if !this.skipErr{
			return
		}

		return
	}

	res.childMap = make(map[string]*HtmlPageResult)

	res.childMap[node.name] = newRes

	this.handleNode(doc.Selection, nodeIndex + 1, newRes)



}



func (this *HtmlPageAnalyzer) executeGet(curr *goquery.Selection, nodeIndex int, res *HtmlPageResult){

	node := this.nodes[nodeIndex]

	this.info.Push(node.name)
	defer this.info.Pop()

	ret, err := GetItemFromSelection(node.rule, curr)
	if err != nil{
		newErr := common.ErrorLog(PrintInfoStack(this.info), err)
		this.errs = append(this.errs, newErr)

		return
	}

	if res.Vals == nil{
		res.Vals = make(map[string]string)

	}
	res.Vals[node.name] = ret

	this.handleNode(curr, nodeIndex + 1, res)

}

func (this *HtmlPageAnalyzer) executeHtml(curr *goquery.Selection, nodeIndex int, res *HtmlPageResult){

	node := this.nodes[nodeIndex]

	this.info.Push(node.name)
	defer this.info.Pop()

	var err error
	res.html, err = curr.Html()
	if err != nil{

		this.errs = append(this.errs, common.ErrorLog("get html failed", PrintInfoStack(this.info)))
		return
	}


	this.handleNode(curr, nodeIndex + 1, res)

}





func GetItemFromSelection(ruleObj *RuleObject, s *goquery.Selection) (string, error){

	var ok bool
	var err error
	var item string

	if ruleObj.ValType == Rule_Value_Type_Attr{
		item, ok = s.Find(ruleObj.Key).Attr(ruleObj.Val)
		if ok == false{
			return "", common.ErrorLog("attr find failed", ruleObj.String())
		}
	}else if ruleObj.ValType == Rule_Value_Type_Text{
		item = s.Find(ruleObj.Key).Text()
		if len(item) <= 0{
			return "", common.ErrorLog("text find failed", ruleObj.String())
		}
	}else if ruleObj.ValType == Rule_Value_Type_Html{
		item, err = s.Find(ruleObj.Key).Html()
		if err !=nil {
			return "", common.ErrorLog("html find failed", ruleObj.String(), err)
		}
	}else if ruleObj.ValType == Rule_Value_Type_MultiText{
		nodes := s.Find(ruleObj.Key)

		buff := new(bytes.Buffer)

		nodes.Each(func(index int, s *goquery.Selection){

			buff.WriteString(s.Text())
		})

		item = buff.String()

		if len(item) <= 0{
			return "", common.ErrorLog("text find failed", ruleObj.String())
		}

	}

	return item, nil
}


























