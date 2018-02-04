package orm

import (
	"errors"
	"reflect"
	"MarsXserver/common"
	"strconv"
	"time"
)



type OrmSendExprFunc func(expr *XOrmEprData, retCh chan interface{})
type OrmHandleBytesRspFuncInf func(buf *common.MBuffer, expr *XOrmEpr) ([]interface{}, error)

var (

	models       map[string]*DBModelInfo = make(map[string]*DBModelInfo)

)

const (

	CacheData_Expire_Duration = time.Hour * 1

)



type CacheData struct {

	tm time.Time
	data interface{}

}

type XOrm struct{
	driver       OrmDriver

	sendExprFunc OrmSendExprFunc
	handleBytesRspFunc OrmHandleBytesRspFuncInf

	cache map[string]CacheData

	IsServer bool

}



func NewOrm(config *OrmConfigData) (orm *XOrm, err error){


	if config != nil{

		return NewServerOrm(config)

	}else{

		return NewClientOrm()
	}


}



func NewClientOrm() (orm *XOrm, err error){

	orm = &XOrm{
		IsServer: false,
		cache: make(map[string]CacheData),
	}

	return
}



func NewServerOrm(config *OrmConfigData) (orm *XOrm, err error){
	ormDrivers.lock.RLock()
	defer ormDrivers.lock.RUnlock()

	dvr, ok := ormDrivers.drivers[config.DriverName]
	if ok != true{
		common.ErrorLog("driver is not exists:", config.DriverName)
		return nil, errors.New("driver is not exists")
	}

	if err := dvr.open(config.ConnectionInfo); err != nil{
		return nil, err
	}

	orm = &XOrm{
		driver:   dvr,
		IsServer: true,
	}
	return
}

func (this *XOrm) RegisterSendFunc(sendFunc OrmSendExprFunc){
	this.sendExprFunc = sendFunc
}

func (this *XOrm) RegisterBytesRspFunc(rspFunc OrmHandleBytesRspFuncInf){
	this.handleBytesRspFunc = rspFunc
}



func RegisterModel(models_... interface{}) error{

	for _, model := range  models_{

			//tp := reflect.TypeOf(model)
			val := reflect.ValueOf(model)
			ind := reflect.Indirect(val)

			common.InfoLog("kind:", ind.Kind())

			if ind.Kind() != reflect.Struct{
				common.ErrorLog("model reg can only pass struct")
				return errors.New("model reg can only pass struct")
			}

			tbName := getDBStyleName(ind.Type().Name())

			if _, ok := models[tbName]; ok == true{
			common.InfoLog("model is registered", tbName)
			continue
		}

		modelInfo := &DBModelInfo{
			TableName:   tbName,
			oriPtrValue: val,
			OriType:     ind.Type(),
			fields:      make([]*DBfieldInfo, 0, MAX_COLS_NUM),
			fieldNames:  make([]string, 0, MAX_COLS_NUM),
			FieldDic:    make(map[string]*DBfieldInfo),
			fks:         make(map[string]*DBModelInfo),
		}

		modelInfo.initFields(ind)

		for _, finfo := range modelInfo.fields{

			if finfo.isPk{
				modelInfo.pkField = finfo
				break
			}

		}
		models[tbName] = modelInfo
	}

	return nil

}


func Bootstrap() error{

	for _, model := range models{
		for _, finfo := range model.fields{
			if len(finfo.relName) > 0{
				relModal, ok := models[getDBStyleName(finfo.relName)]
				if ok == false{
					return common.ErrorLog("rel modal is not registered:", finfo.relName)
				}
				finfo.relModel = relModal

			}
		}
	}
	return nil
}

func (this *XOrm) getModel(name string) (model *DBModelInfo, err error){

	model, ok := models[name]
	if ok == false{
		return nil, common.ErrorLog("no such model", name)
	}
	return model, nil
}



func (this *XOrm) getModelFromInterface(data interface{}) (modelInfo *DBModelInfo, err error){

	val := reflect.ValueOf(data)

	ind := reflect.Indirect(val)

	if ind.Type().Kind() == reflect.Ptr{
		return nil, common.ErrorLog("input model type is wrong:", ind.Type().Kind())
	}

	tbName := getDBStyleName(ind.Type().Name())

	modelInfo, err = this.getModel(tbName)
	if err != nil{
		return nil, err
	}
	return
}


func (this *XOrm) CreateAll() error{

	defer func(){
		if err := recover(); err != nil{
			common.ErrorLog("create panic:", err)
		}
	}()

	for tableName := range models{

		modelInfo, err := this.getModel(tableName)
		if err != nil{
			return err
		}

		ok, err := this.driver.Exists(modelInfo)
		if err != nil{
			return err
		}

		if ok {
			common.InfoLog("table existing:", tableName)
			continue
		}

		if err := this.driver.Create(modelInfo); err != nil{
			return common.ErrorLog("create table failed,", tableName, err)
		}
	}


	return nil
}



func (this *XOrm) Save(data interface{}) (lastInsertId int32, err error){

	defer func(){

		if err := recover(); err != nil{
			common.ErrorLog("insert panic:", data, err)
		}
	}()


	ormExpr := this.NewOrmExpr(data)

	retCh := make(chan []interface{})
	go ormExpr.Save().Run(retCh)
	rets, ok := <-retCh

	if ok == false {
		return 0, nil
	}

	if len(rets) != 1{
		return 0, common.ErrorLog(" update res is not one num", rets)
	}

	retVal,ok := rets[0].(int)
	if ok == false{
		return 0, common.ErrorLog("update res is not int", retVal)
	}

	return int32(retVal), nil

}



func (this *XOrm) Read(data interface{}, useCache bool) error{  //check ok

	defer func(){
		if err := recover(); err != nil{
			common.ErrorLog("read obj panic:", data, err)
		}
	}()

	val := reflect.ValueOf(data)

	ind := reflect.Indirect(val)

	if ind.Type().Kind() == reflect.Ptr{
		return common.ErrorLog("input model type is wrong:", ind.Type().Kind())
	}

	ormExpr := this.NewOrmExpr(data)

	pkVal, err := ormExpr.ModelInfo.getPkValue(ind)
	if err != nil{
		return common.ErrorLog("get pk val failed", ormExpr.ModelInfo.TableName)
	}

	cacheKey := ormExpr.ModelInfo.TableName + strconv.Itoa(int(pkVal))

	if useCache{

		if cacheItem, ok :=  this.cache[cacheKey]; ok == true{
			if common.GetTimeNow().Sub(cacheItem.tm) > CacheData_Expire_Duration{
				delete(this.cache, cacheKey)
			}else {
				ind.Set(reflect.Indirect(reflect.ValueOf(cacheItem.data)))
				return nil
			}
		}
	}


	retCh := make(chan []interface{})

	go ormExpr.Filter("id", "=", strconv.FormatInt(int64(pkVal), 10)).List().Run(retCh)

	newDatas := <-retCh


	if len(newDatas) != 1{
		return common.ErrorLog("select Data len error:", len(newDatas))
	}

	ind.Set(reflect.Indirect(reflect.ValueOf(newDatas[0])))

	if !this.IsServer{
		this.cache[cacheKey] = CacheData{data: newDatas[0], tm: common.GetTimeNow()}
	}


	return nil

}


func (this *XOrm) Update(data interface{}) error{

	defer func(){

		if err := recover(); err != nil{
			common.ErrorLog("update panic:", data, err)
		}

	}()

	val := reflect.ValueOf(data)

	ind := reflect.Indirect(val)

	if ind.Type().Kind() == reflect.Ptr{
		return common.ErrorLog("input model type is wrong:", ind.Type().Kind())
	}


	ormExpr := this.NewOrmExpr(data)

	pkVal, err := ormExpr.ModelInfo.getPkValue(ind)
	if err != nil{
		return common.ErrorLog("get pk val failed", ormExpr.ModelInfo.TableName)
	}

	cacheKey := ormExpr.ModelInfo.TableName + strconv.Itoa(int(pkVal))

	retCh := make(chan []interface{})
	go ormExpr.Update().Run(retCh)
	rets, ok := <-retCh

	if ok == false {
		return nil
	}

	if len(rets) != 1{
		return common.ErrorLog(" update res is not one num", rets)
	}

	retInf,ok := rets[0].(int)
	if ok == false{
		return common.ErrorLog("update res is not int", retInf)
	}

	if !this.IsServer{
		this.cache[cacheKey] = CacheData{data: data, tm: common.GetTimeNow()}
	}

	return nil


}


func (this *XOrm) Delete(data interface{}) error{

	defer func(){

		if err := recover(); err != nil{
			common.ErrorLog("delete panic:", data, err)
		}

	}()

	ormExpr := this.NewOrmExpr(data)

	retCh := make(chan []interface{})
	go ormExpr.Delete().Run(retCh)
	rets, ok := <-retCh

	if ok == false {
		return nil
	}

	if len(rets) != 1{
		return common.ErrorLog(" update res is not one num", rets)
	}

	retInf,ok := rets[0].(int)
	if ok == false{
		return common.ErrorLog("update res is not int", retInf)
	}

	return nil

}

func (this *XOrm) Count(data interface{}) (count int, err error){

	defer func(){

		if err := recover(); err != nil{
			common.ErrorLog("count panic:", data, err)
		}

	}()


	val := reflect.ValueOf(data)

	ind := reflect.Indirect(val)

	if ind.Type().Kind() == reflect.Ptr{
		return 0, common.ErrorLog("input model type is wrong:", ind.Type().Kind())
	}

	tbName := getDBStyleName(ind.Type().Name())

	modelInfo, err := this.getModel(tbName)
	if err != nil{
		return 0, err
	}

	count, err = this.driver.Count(modelInfo)
	if err != nil{
		common.ErrorLog("count failed:", modelInfo.TableName)
		return 0, err
	}

	return

}





func (this *XOrm) List(data interface{}, limit int, offset int) (items []interface{}, err error){

	defer func(){
		if err := recover(); err != nil{
			common.ErrorLog("list panic:", data, err)
		}
	}()

	ormExpr := this.NewOrmExpr(data)

	retCh := make(chan []interface{})
	go ormExpr.List().Limit(limit, offset).Run(retCh)
	rets, ok := <-retCh

	if ok == false || err != nil{
		return nil, err
	}

	items = make([]interface{}, 0, len(rets))

	for _, data := range rets{

		items = append(items, data)

	}


	return

}










