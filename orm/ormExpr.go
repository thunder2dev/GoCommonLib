package orm

import (
	"MarsXserver/common"
	"reflect"
	"fmt"
	"time"
	"strings"
	"strconv"
)

type XOrmOpType int


const (
	_ XOrmOpType = iota
	OP_INSERT
	OP_SELECT
	OP_UPDATE
	OP_DELETE

)


type XOrmOrderType string

const (
	_ XOrmOrderType = ""
	ORDER_NONE	= ""
	ORDER_ASC	= "ASC"
	ORDER_DES	= "DESC"
)


type XOrmEprData struct {
	OpType int

	ModelName string

	ColNames  []string
	ColValues []string

	RetNames []string

	Filters     []string
	FilterNames []string

	Limit  int
	Offset int

	IsReturnFullMoel bool

	OrderType string
	OrderCol  string


}


type XOrmEpr struct {

	ModelInfo *DBModelInfo
	oriData interface{}
	Data      *XOrmEprData

	Orm *XOrm
	err error

}


func (this *XOrm) NewOrmExprFromExprData(data *XOrmEprData) *XOrmEpr{

	expr := &XOrmEpr{
		Data: data,
		Orm: this,
	}

	tbName := data.ModelName

	modelInfo, ok := models[tbName]
	if ok == false{
		common.ErrorLog("no table named:", tbName)
		return nil
	}

	expr.ModelInfo = modelInfo

	return expr


}


func (this *XOrm)NewOrmExpr(data interface{}) *XOrmEpr{

	ormExpr := &XOrmEpr{
		Orm: this,
		oriData: data,
		Data: &XOrmEprData{
			ColNames:    nil,
			ColValues:   nil,
			RetNames:    nil,
			FilterNames: make([]string, 0, 1),
			OrderType:   string(ORDER_NONE),
		},

	}

	val := reflect.ValueOf(data)

	ind := reflect.Indirect(val)

	if ind.Type().Kind() == reflect.Ptr{
		ormExpr.err = common.ErrorLog("input model type is wrong:", ind.Type().Kind())
		return ormExpr
	}

	tbName := getDBStyleName(ind.Type().Name())

	modelInfo, err := this.getModel(tbName)
	if err != nil{
		ormExpr.err = err
		return ormExpr
	}

	/*
	ormExpr.Data.ColNames, ormExpr.Data.ColValues, err = modelInfo.getInputValues(data, true)
	if err != nil{
		ormExpr.err = err
		return ormExpr
	}*/

	ormExpr.Data.ModelName = tbName
	ormExpr.ModelInfo = modelInfo

	return ormExpr

}

func (this *XOrmEpr) Update() *XOrmEpr{

	if this.err != nil{
		return this
	}

	this.Data.OpType = int(OP_UPDATE)

	colNames, colVals, err := this.getInputValues(this.oriData, false)
	if err != nil{
		this.err = common.ErrorLog("get input cols failed")
		return this
	}

	this.Data.ColNames = colNames
	this.Data.ColValues = colVals

	return this
}


func (this *XOrmEpr) Save() *XOrmEpr{

	if this.err != nil{
		return this
	}

	this.Data.OpType = int(OP_INSERT)

	colNames, colVals, err := this.getInputValues(this.oriData, false)
	if err != nil{
		this.err = common.ErrorLog("get input cols failed")
		return this
	}

	this.Data.ColNames = colNames
	this.Data.ColValues = colVals


	return this
}

func (this *XOrmEpr) Delete() *XOrmEpr{
	if this.err != nil{
		return this
	}

	this.Data.OpType = int(OP_DELETE)

	colNames, colVals, err := this.getInputValues(this.oriData, false)
	if err != nil{
		this.err = common.ErrorLog("get input cols failed")
		return this
	}

	this.Data.ColNames = colNames
	this.Data.ColValues = colVals

	return this
}

func (this *XOrmEpr) Read() *XOrmEpr{

	if this.err != nil{
		return this
	}

	this.Data.OpType = int(OP_SELECT)

	colNames, colVals, err := this.getInputValues(this.oriData, true)
	if err != nil{
		this.err = common.ErrorLog("get input cols failed")
		return this
	}

	this.Data.ColNames = colNames
	this.Data.ColValues = colVals


	return this
}



func (this *XOrmEpr) List(cols...string) *XOrmEpr{

	if this.err != nil{
		return this
	}

	this.Data.OpType = int(OP_SELECT)

	if len(cols) <= 0{
		this.Data.IsReturnFullMoel = true
		return this
	}

	this.Data.RetNames = make([]string, 0, len(cols))

	for _, colName := range cols{

		this.Data.RetNames = append(this.Data.RetNames, colName)
	}

	return this

}

func (this *XOrmEpr) AddRetCol(col string) *XOrmEpr{
	if this.err != nil{
		return this
	}

	if this.Data.RetNames == nil{
		this.Data.RetNames = make([]string, 0)
	}

	for _, rn := range this.Data.RetNames {
		if rn == col{
			return this
		}
	}


	this.Data.RetNames = append(this.Data.RetNames, col)

	this.Data.IsReturnFullMoel = false

	return this
}

func (this *XOrmEpr) HasRetCol(col string) bool{

	for _, cc := range this.Data.RetNames {
		if cc == col {
			return true
		}
	}
	return false
}


func (this *XOrmEpr) ClearRetCols() *XOrmEpr{
	if this.err != nil{
		return this
	}

	this.Data.RetNames = nil

	this.Data.IsReturnFullMoel = true

	return this
}


func (this *XOrmEpr) Filter(col string, cond string, val string) *XOrmEpr{

	if this.err != nil{
		return this
	}

	filterName := fmt.Sprintf("%s%s", col, cond)
	filter := fmt.Sprintf("%s%s%s", col, cond, val)

	for ii, ff := range this.Data.Filters {
		if strings.Contains(ff, filterName){
			common.InfoLog("filter overlapped", filter)
			this.Data.Filters[ii] = filter
			return this
		}
	}

	this.Data.FilterNames = append(this.Data.FilterNames, filterName)
	this.Data.Filters = append(this.Data.Filters, filter)

	return this

}





func (this *XOrmEpr) Limit(limit int, offset int)*XOrmEpr{

	if this.err != nil{
		return this
	}

	this.Data.Limit = limit
	this.Data.Offset = offset

	return this
}


func (this *XOrmEpr) Order(col string, asc bool)*XOrmEpr{

	if this.err != nil{
		return this
	}

	this.Data.OrderCol = col

	if asc {
		this.Data.OrderType = ORDER_ASC
	}else{
		this.Data.OrderType = ORDER_DES
	}
	return this
}


func (this *XOrmEpr) RunDirectReturn() ([]interface{}, error){

	retCh := make(chan []interface{})
	go this.Run(retCh)
	arr := <- retCh

	return arr, nil
}



func (this *XOrmEpr) Run(retCh chan []interface{}) { //go routine
	defer func(){
		if err := recover(); err != nil{
			close(retCh)
			common.ErrorLog("run expr panic", this.ModelInfo.TableName, err)
		}
	}()

	if this.Orm.IsServer {

		rets, err := this.Orm.Run(this)
		if err != nil{
			close(retCh)
			common.ErrorLog("orm server run ", this.ModelInfo.TableName, " op:", this.Data.OpType, "failed")
			return

		}

		retCh <- rets
		return
	}

	passedAll := false

	for{

		if this.err != nil{
			common.ErrorLog("expr is error")
			break
		}

		innerCh := make(chan interface{})

		this.Orm.sendExprFunc(this.Data, innerCh)

		rspBytesInf, ok := <- innerCh

		if ok == false{
			common.ErrorLog("table:", this.Data.ModelName, " rsp is close")
			break
		}

		buf, ok := rspBytesInf.(*common.MBuffer)
		if ok == false{
			common.ErrorLog("db rsp is not []byte")
			break
		}

		arr, err := this.Orm.handleBytesRspFunc(buf, this)
		if err  != nil{
			common.ErrorLog("handler bytes rsp failed", err, this.Data.ModelName)
			break
		}

		retCh <- arr

		passedAll = true
		break
	}

	if !passedAll{
		close(retCh)
	}
}



func (this *XOrm) Run(epr *XOrmEpr) (ret []interface{}, err error){

	defer func(){
		if err := recover(); err != nil{
			common.ErrorLog("read panic:", epr.Data.ModelName, err)
		}
	}()

	var pageClause string

	if epr.Data.Limit > 0{
		pageClause = fmt.Sprintf("LIMIT %d OFFSET %d", epr.Data.Limit, epr.Data.Offset)
	}

	var orderClause string

	if epr.Data.OrderType != ORDER_NONE{
		orderClause = fmt.Sprintf("ORDER BY %s %s", epr.Data.OrderCol, epr.Data.OrderType)
	}else{
		orderClause = ""
	}

	switch epr.Data.OpType {
	case int(OP_SELECT):
		retVals, err := this.driver.Select(epr, epr.Data.ColNames, epr.Data.ColValues, epr.Data.RetNames, epr.Data.Filters, pageClause, orderClause)
		if err != nil{
			return nil, err
		}
		if epr.Data.IsReturnFullMoel {
			ret = make([]interface{}, len(retVals))
			for ii:= 0; ii < len(retVals); ii++{
				ret[ii] = retVals[ii].(reflect.Value).Interface()
			}
			return ret, nil
		}
		return retVals, nil
	case int(OP_INSERT):
		lastInsertId, err:= this.driver.Insert(epr.ModelInfo, epr.Data.ColNames, epr.Data.ColValues)
		if err != nil{
			return nil, err
		}

		return []interface{}{lastInsertId}, nil
	case int(OP_UPDATE):
		lastInsertId, err:= this.driver.Update(epr.ModelInfo, epr.Data.ColNames, epr.Data.ColValues)
		if err != nil{
			return nil, err
		}

		return []interface{}{lastInsertId}, nil
	case int(OP_DELETE):
		delNum, err:= this.driver.Delete(epr.ModelInfo, epr.Data.ColNames, epr.Data.ColValues)
		if err != nil{
			return nil, err
		}

		return []interface{}{delNum}, nil


	default:
		return nil, common.ErrorLog("op type is not defined")

	}
	return

}


func containsString(ss []string, s string) bool{

	for _, val := range ss{

		if val == s{
			return true
		}

	}

	return false
}


func (this *XOrmEpr) AsyncMultiRunDefault(retCh chan []interface{}, outErr *error ){

	this.AsyncMultiRun(common.DefaultAsyncReadLimit, common.DefaultAsyncReadInterval, retCh, outErr)

}



func (this *XOrmEpr) AsyncMultiRun(count int, interval time.Duration, retCh chan []interface{}, outErr *error ){ //go routine

	defer func(){

		common.SafeCloseArrChannel(retCh)

		if err := recover(); err != nil{
			*outErr = common.ErrorLog("panic:", err)
		}
	}()


	tick := time.NewTicker(interval)

	defer tick.Stop()

	oldLimit := 0

	if this.Data.Limit > 0{
		oldLimit = this.Data.Limit
	}


	retNamesBak := this.Data.RetNames
	isRetFullModalBak := this.Data.IsReturnFullMoel

	retCha := make(chan []interface{})
	go this.ClearRetCols().AddRetCol("id").Order("id", true).Limit(1, 0).Run(retCha)
	rets, ok := <-retCha

	if ok == false || rets == nil{
		*outErr = common.ErrorLog("get first record failed:", this.Data.ModelName)
		return
	}

	if len(rets) == 0{
		common.InfoLog("get first id zero", this.Data.ModelName)
		return
	}

	_firstId := rets[0].(map[string]interface{})["id"]
	firstId := _firstId.(int32)


	var _lastId interface{}
	lastId := firstId

	if oldLimit <= 0{
		go this.Order("id", false).Limit(1, 0).Run(retCha)
		rets, ok = <-retCha

		if ok == false || rets == nil || len(rets) != 1{
			*outErr = common.ErrorLog("get last record when oldlimit 0 failed:", this.Data.ModelName)
			return
		}

		_lastId = rets[0].(map[string]interface{})["id"]
	}else if oldLimit > 1{
		go this.Order("id", true).Limit(1, oldLimit - 1).Run(retCha)
		rets, ok = <-retCha

		if ok == false || rets == nil || len(rets) > 1 || len(rets) < 0{
			*outErr = common.ErrorLog("get last record when oldlimit > 1 failed:", this.Data.ModelName)
			return
		}else if len(rets) == 0{
			go this.Order("id", false).Limit(1, 0).Run(retCha)
			rets, ok = <-retCha

			if ok == false || rets == nil || len(rets) != 1{
				*outErr = common.ErrorLog("get last record when oldlimit is too big failed:", this.Data.ModelName)
				return
			}
			_lastId = rets[0].(map[string]interface{})["id"]
		}else{
			_lastId = rets[0].(map[string]interface{})["id"]
		}
	}


	lastId = _lastId.(int32)

	if lastId - firstId < 0 {
		*outErr = common.ErrorLog("first id :", firstId, " larger than last id:", lastId)
		return
	}

	this.Data.IsReturnFullMoel = isRetFullModalBak
	this.Data.RetNames = retNamesBak
	offsetId := firstId - 1

	for{
		currCnt := int32(count)
		if lastId - offsetId < currCnt{
			currCnt = lastId - offsetId
		}

		if currCnt <= 0{
			return
		}

		if !this.Data.IsReturnFullMoel && !this.HasRetCol("id"){
			this.AddRetCol("id")
		}

		go this.Filter("id", ">", strconv.Itoa(int(offsetId))).Order("id", true).Limit(int(currCnt), 0).Run(retCha)
		rets, ok = <-retCha

		if ok == false || rets == nil || len(rets) <= 0{
			*outErr = common.ErrorLog("get chunk Data failed", this.Data.ModelName, " from id:", offsetId, " count:", currCnt)
			return
		}

		var maxId int32

		for _, ret := range rets{

			var idCol interface{}
			var err error

			if this.Data.IsReturnFullMoel {
				idCol, err = common.GetFieldValueByInterface(ret, "Id")
				if err != nil{
					*outErr = common.ErrorLog("get field failed:", "Id")
					return
				}
			}else{
				ret_obj := ret.(map[string]interface{})
				idCol = ret_obj["id"]
			}


			switch id_int:=idCol.(type) {
			case int32:
				id_64 := int64(id_int)
				if id_64 > int64(maxId){
					maxId = int32(id_64)
				}
			case int64:
				if id_int > int64(maxId){
					maxId = int32(id_int)
				}
			default:
				*outErr = common.ErrorLog("id is not int32 or 64", id_int)
				return
			}
		}

		if maxId <= 0{
			*outErr = common.ErrorLog("this turn got nothing")
			return
		}

		offsetId = maxId

		retCh <- rets

		if len(rets) <= 0 || offsetId >= lastId{
			return
		}

		<-tick.C
	}

}


























