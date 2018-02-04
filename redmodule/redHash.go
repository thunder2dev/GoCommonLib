package redmodule

import (
	"time"
	"reflect"
	"MarsXserver/common"
	"fmt"
	"bytes"
	"strconv"
	"strings"
)



const (
	Idx_Ext_Str = "_Idx"
	Cnt_Ext_Str = "_Cnt"
)


var (
	hashModels	map[string]*RedHashModel
	redName2structName map[string]string
)

func init(){

	hashModels = make(map[string]*RedHashModel)
	redName2structName = make(map[string]string)

}


type RedHashValField struct {

	ValType RedValueType

}


type RedHashModel struct {

	redNamePrefix string

	oriPtrValue reflect.Value
	oriType reflect.Type

	allFieldNames    map[string]bool
	allValFieldNames []string

	keyNamePattern string
	countkeyPatterns []string

	ValFields map[string]*RedHashValField

	IdFields []string

	cntIdxs []int
	cntType RedCountType

	Timestamp time.Time

	IsAutoIncr bool

	HasParent bool
	ParentName string
	Children []*RedHashModel

	timeGc *TimeGcModel

	addFunc func(model RedModel, objPtr interface{}) error
	delFunc func(model RedModel, objPtr interface{}) error  //objPtr only contains id fields

}


func NewRedHashModel() *RedHashModel{

	redHash := &RedHashModel{
		allFieldNames: make(map[string]bool),
		allValFieldNames: make([]string, 0),
		countkeyPatterns: make([]string, 0),
		ValFields:        make(map[string]*RedHashValField),
		IsAutoIncr:       false,
		HasParent:        false,
		Children:         make([]*RedHashModel, 0),
	}

	return redHash

}



func GetHashModelbyRedName(redName string) (*RedHashModel, error){

	structName, ok := redName2structName[redName]
	if ok == false{
		return nil, common.ErrorLog("no struct name reg", redName)
	}

	model, ok := hashModels[structName]
	if ok == false{
		return nil, common.ErrorLog("no hash model reg", structName)
	}

	return model, nil
}

func RedKeySplit(keyName string) (Prefix, IdPart string){

	idx := strings.Index(keyName, "_")

	if idx < 0{
		common.ErrorLog("split key failed", keyName)
		return keyName, ""
	}

	return keyName[0:idx], keyName[idx:]

}


func RedKeySplitReverse(keyName string) (Prefix, IdPart string, err error){

	idx := strings.LastIndex(keyName, "_")

	if idx < 0{
		return keyName, "",common.ErrorLog("split key failed", keyName)
	}

	return keyName[0:idx], keyName[idx:], nil

}

func RedKeySplitFree(keyName string, order int, count int) (Prefix, IdPart string, err error){   //order 0: Key; order 1: Key_1

	keyParts := strings.Split(keyName, "_")

	if order == len(keyParts) -1{
		return keyName, "", nil
	}

	if order + count > len(keyParts) - 1{
		return "", "", common.ErrorLog("order err", keyName, order, count)
	}

	if order == 0{
		Prefix = keyParts[0]
	}else {
		Prefix = strings.Join(keyParts[0:order+1], "_")
	}

	if count == 1{
		IdPart = "_" + keyParts[order+1]
	}else {
		IdPart = strings.Join(keyParts[order+1:order+count], "_")
	}

	return Prefix, IdPart, nil

}


func RegisterHashModel(keyPrefix string, objPtr interface{}, idFields []string, cntIdxs []int, cntType RedCountType, isAuto bool, hasParent bool, parentRedName string, gcDuration time.Duration) error{

	objPtrValue := reflect.ValueOf(objPtr)

	if objPtrValue.Kind() != reflect.Ptr{
		return common.ErrorLog("not a pointer")
	}

	model := NewRedHashModel()

	objValue := reflect.Indirect(objPtrValue)
	objType := objValue.Type()

	model.oriPtrValue = objPtrValue
	model.oriType = objType

	structName := objType.Name()

	model.redNamePrefix = keyPrefix

	redName2structName[keyPrefix] = structName


	for ii := 0; ii < objValue.NumField(); ii++{
		fieldInfo := objType.Field(ii)
		model.allFieldNames[fieldInfo.Name] = true

		if !common.StringInSlice(fieldInfo.Name, idFields){
			model.allValFieldNames = append(model.allValFieldNames, fieldInfo.Name)
		}
	}

	if len(idFields) <= 0{
		return common.ErrorLog("id fields cannot be less than 1")
	}


	namePattern := new(bytes.Buffer)
	namePattern.WriteString(keyPrefix)

	for ii := 0; ii < len(idFields); ii++{

		model.countkeyPatterns = append(model.countkeyPatterns, namePattern.String())

		fieldName := idFields[ii]
		if _, ok := model.allFieldNames[fieldName]; ok == false{
			return common.ErrorLog("field " + fieldName + " is not is struct " + structName)
		}

		namePattern.WriteString("_%v")

	}

	model.keyNamePattern = namePattern.String()
	model.IdFields = idFields
	model.cntIdxs = cntIdxs
	model.cntType = cntType

	for _, fieldName := range model.allValFieldNames{
		if _, ok := model.allFieldNames[fieldName]; ok == false{
			return common.ErrorLog("field " + fieldName + " is not is struct " + structName)
		}

		fieldInfo := objValue.FieldByName(fieldName)

		newField := new(RedHashValField)

		switch fieldInfo.Kind() {

		case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64, reflect.Int32, reflect.Uint32:
			newField.ValType = RedValueTypeInt
		case reflect.String:
			newField.ValType = RedValueTypeString
		case reflect.Struct:
			newField.ValType = RedValueTypeTime
		default:
			return common.ErrorLog("field " + fieldName + " type is not num nor string")
		}

		model.ValFields[fieldName] = newField
	}


	if isAuto{

		model.IsAutoIncr = true

	}

	model.HasParent = hasParent
	model.ParentName = parentRedName

	if hasParent{

		parentModel, err := GetHashModelbyRedName(parentRedName)
		if err != nil{
			return err
		}

		parentModel.Children = append(parentModel.Children, model)
	}

	if gcDuration > 0{
		model.timeGc = RegiterTimeGcTable(model, TimeGcNoUpdate, gcDuration)
	}


	hashModels[structName] = model

	return nil

}


func RegisterHashAddDelFunc(tbName string, addFunc, delFunc func(model RedModel, objInf interface{})error) error{

	model, err := GetHashModelbyRedName(tbName)
	if err != nil{
		return err
	}

	model.addFunc = addFunc
	model.delFunc = delFunc

	return nil
}


func (this *RedHashModel) GetTableType() RedTableType{

	return RedTableHash

}

func (this *RedHashModel) GetTableName() string{

	return this.redNamePrefix

}


func (this *RedHashModel) SetIdVals(objVal reflect.Value, idVals []int64) error{

	for ii := 0; ii < len(idVals); ii++ {
		idName := this.IdFields[ii]
		idValField := objVal.FieldByName(idName)
		idValInd := reflect.Indirect(idValField)

		switch idValInd.Kind() {
		case reflect.Int, reflect.Int32, reflect.Int64:
			idValInd.SetInt(idVals[ii])
		default:
			return common.ErrorLog("field is not int", )
		}
	}
	return nil
}



func (this *RedHashModel) GetIdVals(objVal reflect.Value, idx int, isCreate bool) ([]int64, error){

	idVals := make([]int64, 0)
	var tmpVal int64

	for ii := 0; ii < idx; ii++ {
		idName := this.IdFields[ii]
		idValField := objVal.FieldByName(idName)
		idValInd := reflect.Indirect(idValField)
		idValInf := idValInd.Interface()

		if common.IsDefaultValueOfType(idValInf) && (!isCreate || ii != len(this.IdFields) -1) {
			return nil, common.ErrorLog("get key name to idx failed, empty val", idName, idx)
		}

		switch idValInd.Kind() {
		case reflect.Int, reflect.Int32, reflect.Int64:
			tmpVal = idValInd.Int()
		default:
			return nil, common.ErrorLog("field is not int", idValInf)
		}

		idVals = append(idVals, tmpVal)
	}

	return idVals, nil
}


func (this *RedHashModel) GetKeyNameToIdxFromIdVals(idVals []int64, idx int) (string, error){

	var tmp []interface{}

	for ii:=0; ii<idx; ii++{
		tmp = append(tmp, idVals[ii])
	}


	if idx >= len(this.IdFields){
		return fmt.Sprintf(this.keyNamePattern, tmp...), nil
	}
	return fmt.Sprintf(this.countkeyPatterns[idx], tmp...), nil
}



func (this *RedHashModel) GetKeyNameToIdxFromValobj(objVal reflect.Value, idx int) (string, error){

	idVals := make([]interface{}, 0)

	for ii := 0; ii < idx; ii++ {
		idName := this.IdFields[ii]
		idValField := objVal.FieldByName(idName)
		idValInd := reflect.Indirect(idValField)
		idValInf := idValInd.Interface()

		if common.IsDefaultValueOfType(idValInf){
			return "", common.ErrorLog("get key name to idx failed, empty val", idName, idx)
		}

/*		var inf interface{}

		switch idValField.Kind() {
		case reflect.Int, reflect.Int32, reflect.Int64:
			inf = idValInf.()
		case string:
		}*/


		idVals = append(idVals, idValInf)
	}

	if idx >= len(this.IdFields){
		return fmt.Sprintf(this.keyNamePattern, idVals...), nil
	}
	return fmt.Sprintf(this.countkeyPatterns[idx], idVals...), nil
}


func (this *RedHashModel) GetCountKeyName(objVal reflect.Value, idx int) string{

	cntName, _ := this.GetKeyNameToIdxFromValobj(objVal, idx)
	return fmt.Sprintf("%s%s", cntName, Cnt_Ext_Str)
}


func (this *RedHashModel) GetAutoKeyFromValobj(red *XRedis,objVal reflect.Value) (prefix, full string){

	autoPrefix, _ := this.GetKeyNameToIdxFromValobj(objVal, len(this.IdFields) - 1)

	autoIdxKey := autoPrefix + Idx_Ext_Str

	return autoPrefix, autoIdxKey

}

func (this *RedHashModel) GetAutoKeyFromIdVals(red *XRedis, idVals []int64) (prefix, full string){

	autoPrefix, _ := this.GetKeyNameToIdxFromIdVals(idVals, len(this.IdFields) - 1)

	autoIdxKey := autoPrefix + Idx_Ext_Str

	return autoPrefix, autoIdxKey

}



/*
func (this *RedHashModel) GetRootKey(red *XRedis, objVal reflect.Value) (full string){

	autoPrefix, _ := this.GetKeyNameToIdxFromValobj(objVal, len(this.IdFields) - 1)

	autoIdxKey := autoPrefix

	return autoIdxKey

}*/

func (this *RedHashModel) GenerateAutoKeyNameFromIdVals(red *XRedis, idVals []int64) string{

	autoPrefix, autoIdxKey := this.GetAutoKeyFromIdVals(red, idVals)

	idxId := red.Conn.Incr(autoIdxKey).Val()

	rootKey := autoPrefix

	if this.HasParent == false && this.timeGc != nil{

		this.timeGc.AddTimeGcKey(red, rootKey)

	}

	idVals[len(idVals) - 1] = idxId

	//idFieldName := this.IdFields[len(this.IdFields) -1]

	//objVal.FieldByName(idFieldName).Set(reflect.ValueOf(int(idxId)))    //must int

	return fmt.Sprintf("%s_%d", autoPrefix, idxId)
}


func (this *RedHashModel) GenerateAutoKeyNameFromValobj(red *XRedis,objVal reflect.Value) string{

	autoPrefix, autoIdxKey := this.GetAutoKeyFromValobj(red, objVal)

	idxId := red.Conn.Incr(autoIdxKey).Val()

	rootKey := autoPrefix

	if this.HasParent == false && this.timeGc != nil{

		this.timeGc.AddTimeGcKey(red, rootKey)

	}

	idFieldName := this.IdFields[len(this.IdFields) -1]

	objVal.FieldByName(idFieldName).Set(reflect.ValueOf(int(idxId)))    //must int

	return fmt.Sprintf("%s_%d", autoPrefix, idxId)
}

func (this *RedHashModel) GetKeyNameFromIdVals(red *XRedis, idVals []int64, isCreate bool ) string{

	keyName := ""

	if this.IsAutoIncr && isCreate {
		return this.GenerateAutoKeyNameFromIdVals(red, idVals)
	}

	keyName, _ = this.GetKeyNameToIdxFromIdVals(idVals, len(this.IdFields))
	return keyName
}

func (this *RedHashModel) GetKeyNameFromValobj(red *XRedis,objVal reflect.Value, isCreate bool ) string{

	keyName := ""

	if this.IsAutoIncr && isCreate {
		return this.GenerateAutoKeyNameFromValobj(red, objVal)
	}

	keyName, _ = this.GetKeyNameToIdxFromValobj(objVal, len(this.IdFields))
	return keyName
}

func (this *RedHashModel) IsValFieldName(fieldName string) bool{

	if _, ok := this.ValFields[fieldName]; ok == false{

		return false;
	}


	return true

}



func getHashModelbyStructName(structName string) (*RedHashModel, error){

	model, ok := hashModels[structName]
	if model == nil || ok == false{
		return nil, common.ErrorLog("get hash model nil ", structName)
	}

	return model, nil

}


func (this *XRedis) Exists(objPtr interface{}) bool{

	objPtrVal := reflect.ValueOf(objPtr)

	if objPtrVal.Kind() != reflect.Ptr{

		common.ErrorLog("objPtr is not ptr")
		return false
	}

	objVal := reflect.Indirect(objPtrVal)
	objType := objVal.Type()

	structName := objType.Name()

	model, err := getHashModelbyStructName(structName)
	if err != nil{
		return false
	}

	keyName := model.GetKeyNameFromValobj(this, objVal, false)

	return this.Conn.Exists(keyName).Val() == 1


}

func (this *XRedis) ReadHashServer(structName string, idVals []int64) interface{}{

	model, err := getHashModelbyStructName(structName)
	if err != nil{
		return err
	}

	keyName, err := model.GetKeyNameToIdxFromIdVals(idVals, len(model.IdFields))
	if err != nil{
		return err
	}

	vals, err := this.Conn.HMGet(keyName, model.allValFieldNames...).Result()
	if err != nil{
		return common.ErrorLog("get m hash vals err", keyName)
	}

	newInf := reflect.New(model.oriType)
	newInd := reflect.Indirect(newInf)

	for ii, fieldName := range model.allValFieldNames{

		field := newInd.FieldByName(fieldName)
		valField, ok := model.ValFields[fieldName]
		if ok == false{
			return common.ErrorLog("not in val fields", keyName, fieldName)
		}

		inf := vals[ii]
		switch  valField.ValType{
		case RedValueTypeInt:
			numStr, ok := inf.(string)
			if !ok {
				return common.ErrorLog("not string", keyName, fieldName, inf)
			}
			num, err := strconv.Atoi(numStr)
			if err != nil{
				return common.ErrorLog("not num string", keyName, fieldName, inf)
			}

			field.SetInt(int64(num))
		case RedValueTypeString:
			field.Set(reflect.ValueOf(inf))
		case RedValueTypeTime:
			timeStr, ok := inf.(string)
			if ok == false{
				return common.ErrorLog("not time type", keyName, fieldName)
			}

			tm, err := time.Parse("20060102150405", timeStr)
			if err != nil{
				return common.ErrorLog("time parse err", keyName, fieldName)
			}

			field.Set(reflect.ValueOf(tm))
		default:
			return common.ErrorLog("field type err", keyName, fieldName)
		}
	}

	for ii, idFieldName := range model.IdFields{
		newInd.FieldByName(idFieldName).SetInt(idVals[ii])
	}



	return newInd.Interface()


}


func (this *XRedis) ReadHash(objPtrs... interface{}) error{

	rootStructName := ""
	var model *RedHashModel
	var err error
	structNameArr := make([]string, len(objPtrs))
	idValsArr := make([][]int64, len(objPtrs))
	objValsArr := make([]reflect.Value, len(objPtrs))

	for ii, objPtr := range objPtrs{
		objPtrVal := reflect.ValueOf(objPtr)

		if objPtrVal.Kind() != reflect.Ptr{

			return common.ErrorLog("objPtr is not ptr")
		}

		objVal := reflect.Indirect(objPtrVal)
		objType := objVal.Type()

		structName := objType.Name()
		if len(rootStructName) <= 0{
			rootStructName = structName
			model, err = getHashModelbyStructName(structName)
			if err != nil{
				return err
			}
		}else if rootStructName != structName{
			return common.ErrorLog("objs is not of same model")
		}

		idVals, err := model.GetIdVals(objVal, len(model.IdFields), false)
		if err != nil{
			return err
		}
		structNameArr[ii] = structName
		idValsArr[ii] = idVals
		objValsArr[ii] = objVal
	}


	if this.IsServer{
		return common.ErrorLog("server should not call this method")
	}

	redRequest := new(RedDataRequest)
	redRequest.StructNames = structNameArr
	redRequest.Op = RedDataOpRead
	redRequest.IdValsArr = idValsArr

	retCh := make(chan interface{})
	this.sendRedFunc(redRequest, retCh)
	rspBytesInf, ok := <-retCh

	if ok == false {
		return common.ErrorLog("rsp chan is closed")
	}

	buf, ok := rspBytesInf.(*common.MBuffer)
	if ok == false{
		return common.ErrorLog("db rsp is not []byte")
	}

	rsp, err := this.handleRedBytesRspFunc(buf, this, model)
	if err  != nil{
		return common.ErrorLog("handler bytes rsp failed, model:", model.redNamePrefix, err)
	}


	rspArr := rsp.Data.([]interface{})

	for ii, rspItem := range rspArr{
		dataPtrVal := reflect.ValueOf(rspItem)
		dataInd := reflect.Indirect(dataPtrVal)
		objValsArr[ii].Set(dataInd)
		if err:= model.SetIdVals(objValsArr[ii], idValsArr[ii]); err != nil{
			return err
		}
	}


	return nil

}

func (this *XRedis) IncrHashFieldBy(objPtr interface{}, fieldName string, offset int) (int, error){

	objPtrVal := reflect.ValueOf(objPtr)

	if objPtrVal.Kind() != reflect.Ptr{

		return -1, common.ErrorLog("objPtr is not ptr")
	}

	objVal := reflect.Indirect(objPtrVal)
	objType := objVal.Type()

	structName := objType.Name()

	model, err := getHashModelbyStructName(structName)
	if err != nil{
		return -1, err
	}

	keyName := model.GetKeyNameFromValobj(this, objVal, false)

	if this.Conn.Exists(keyName).Val() == 0{
		return -1, nil
	}

	res, err := this.Conn.HIncrBy(keyName, fieldName, int64(offset)).Result()
	return int(res), err
}


func (this *XRedis) GetCountServer(structName string, idVals []int64, cntIdx int) (int64, error){

	model, err := getHashModelbyStructName(structName)
	if err != nil{
		return 0, err
	}

	keyName, err := model.GetKeyNameToIdxFromIdVals(idVals, cntIdx)
	if err != nil{
		return 0, err
	}

	var count int64

	if model.cntType == RedCountTypeByNum{
		countStr, err := this.Conn.Get(keyName + "_Cnt").Result()
		if err != nil{
			return 0, nil
		}

		count32, err := strconv.Atoi(countStr)
		count = int64(count32)
		if err != nil{
			return -1, common.ErrorLog("convert count str failed", countStr, keyName)
		}
	}else{
		count, err = this.Conn.SCard(keyName + "_Set").Result()
		if err != nil{
			return 0, nil
		}
	}

	return count, nil


}


func (this *XRedis) GetCount(objPtr interface{}, cntIdx int) (int64, error){

	objPtrVal := reflect.ValueOf(objPtr)

	if objPtrVal.Kind() != reflect.Ptr{

		return -1, common.ErrorLog("objPtr is not ptr")
	}

	objVal := reflect.Indirect(objPtrVal)
	objType := objVal.Type()

	structName := objType.Name()

	model, err := getHashModelbyStructName(structName)
	if err != nil{
		return -1, err
	}

	check := false
	for _, idx := range model.cntIdxs{
		if cntIdx == idx{
			check = true
			break
		}
	}

	if !check{
		return -1, common.ErrorLog("cnt idx not prepared", model.GetTableName(), cntIdx)
	}

	idVals, err := model.GetIdVals(objVal, len(model.IdFields), false)
	if err != nil{
		return 0, err
	}


	if this.IsServer{

		return this.GetCountServer(structName, idVals, cntIdx)

	}else {

		redRequest := new(RedDataRequest)
		redRequest.StructName = structName
		redRequest.Op = RedDataOpCnt
		redRequest.IdVals = idVals
		redRequest.CntIdx = cntIdx

		retCh := make(chan interface{})
		this.sendRedFunc(redRequest, retCh)
		rspBytesInf, ok := <-retCh

		if ok == false {
			return 0, common.ErrorLog("rsp chan is closed")
		}

		buf, ok := rspBytesInf.(*common.MBuffer)
		if ok == false{
			return 0, common.ErrorLog("db rsp is not []byte")
		}

		rsp, err := this.handleRedBytesRspFunc(buf, this, model)
		if err  != nil{
			return 0, common.ErrorLog("handler bytes rsp failed, model:", model.redNamePrefix, err)
		}

		cnt, ok := rsp.Data.(int64)
		if !ok{
			return 0, common.ErrorLog("saved id is not in rsp", structName)
		}

		return cnt, nil
	}

}


func (this *XRedis) SaveHashServer(structName string, idVals []int64, fieldVals []string, isCreate bool) (int64, error){

	model, err := getHashModelbyStructName(structName)
	if err != nil{
		return 0, err
	}

	if len(idVals) != len(model.IdFields) || len(fieldVals) != len(model.ValFields){
		return 0, common.ErrorLog("id vals or field vals cnt err:", len(idVals), len(fieldVals))
	}

	var keyName string

	if isCreate{
		keyName = model.GetKeyNameFromIdVals(this, idVals, true)    //todo the id generator is already increased

		existRes, err := this.Conn.Exists(keyName).Result()
		if err != nil{
			return 0, common.ErrorLog("exist keyname err:", err)
		}

		if existRes == 1{
			return 0, common.ErrorLog("key already existed:", keyName)
		}

		if model.HasParent{

			parentLongKey := keyName

			if len(model.IdFields) > 1{

				parentLongKey, err = model.GetKeyNameToIdxFromIdVals(idVals, len(model.IdFields) -1)
				if err != nil{
					return 0, common.ErrorLog("get parent name failed", structName, err)
				}

			}

			_, parentIdPart := RedKeySplit(parentLongKey)

			parentKey := model.ParentName + parentIdPart

			if this.Conn.Exists(parentKey).Val() == 0{
				return 0, common.ErrorLog("no parent exists", parentLongKey, parentKey)

			}
		}

	}else{
		keyName = model.GetKeyNameFromIdVals(this, idVals, false)
	}

	newInf := reflect.New(model.oriType)
	newInd := reflect.Indirect(newInf)

	valDic := make(map[string]interface{})

	for ii, fieldName := range model.allValFieldNames{

		valField := model.ValFields[fieldName]

		switch valField.ValType {
		case RedValueTypeTime:
			timestamp, err := time.Parse("20060102150405", fieldVals[ii])
			if err != nil{
				return 0, common.ErrorLog("field val is not time", structName, fieldVals[ii])
			}
			valDic[fieldName] = fieldVals[ii]
			newInd.FieldByName(fieldName).Set(reflect.ValueOf(timestamp))
		case RedValueTypeInt:
			intVal, err := strconv.ParseInt(fieldVals[ii], 10, 32)
			if err != nil{
				return 0, common.ErrorLog("field val is not int", structName, fieldVals[ii])
			}
			valDic[fieldName] = intVal
			newInd.FieldByName(fieldName).SetInt(intVal)
		case RedValueTypeInt64:
			intVal, err := strconv.ParseInt(fieldVals[ii], 10, 64)
			if err != nil{
				return 0, common.ErrorLog("field val is not int64", structName, fieldVals[ii])
			}
			valDic[fieldName] = intVal
			newInd.FieldByName(fieldName).SetInt(intVal)
		case RedValueTypeString:
			valDic[fieldName] = fieldVals[ii]
			newInd.FieldByName(fieldName).SetString(fieldVals[ii])
		default:
			return 0, common.ErrorLog("field type not supported")
		}
	}

	for ii, fieldName := range model.IdFields{
		newInd.FieldByName(fieldName).SetInt(idVals[ii])
	}


	this.Conn.HMSet(keyName, valDic)

	for _, cntIdx := range model.cntIdxs{

		splitPart, idPart, err := RedKeySplitFree(keyName, cntIdx, 1)
		if err != nil{
			return 0, common.ErrorLog("split free err", keyName, cntIdx, 1, err)
		}

		if model.cntType == RedCountTypeByNum{
			if err := this.Conn.Incr(splitPart + "_Cnt").Err(); err != nil{
				return 0, common.ErrorLog("incr cnt idx err", splitPart, err)
			}
		}else{

			if err := this.Conn.SAdd(splitPart + "_Set", idPart).Err(); err != nil{
				return 0, common.ErrorLog("add to count set err", splitPart, err)
			}

		}
	}

	if model.addFunc != nil{

		if err := model.addFunc(model, newInf.Interface()); err != nil{
			return 0, common.ErrorLog("add func failed", )
		}

	}

	return  idVals[len(idVals)-1], nil

}



func (this *XRedis) SaveHash(objPtr interface{}, isCreate bool) (int64, error){

	objPtrVal := reflect.ValueOf(objPtr)

	if objPtrVal.Kind() != reflect.Ptr{

		return 0, common.ErrorLog("objPtr is not ptr")
	}

	objVal := reflect.Indirect(objPtrVal)
	objType := objVal.Type()

	structName := objType.Name()

	model, err := getHashModelbyStructName(structName)
	if err != nil{
		return 0, err
	}


	idVals, err := model.GetIdVals(objVal, len(model.IdFields), isCreate)
	if err != nil{
		return 0, err
	}


	filedStrs := make([]string, 0)
	var tmpStr string

	for _, valFieldName := range model.allValFieldNames{

		fval := objVal.FieldByName(valFieldName)

		switch fval.Kind(){
		case reflect.String:
			tmpStr = fval.String()
		case reflect.Int, reflect.Int32, reflect.Int64:
			tmpStr = strconv.FormatInt(fval.Int(), 10)
		case reflect.TypeOf(time.Time{}).Kind():
			timestamp, ok := fval.Interface().(time.Time)
			if ok != true{
				return 0, common.ErrorLog("not timestamp", structName, valFieldName)
			}
			tmpStr = timestamp.Format("20060102150405")
		default:
			return 0, common.ErrorLog("field cannot be format")
		}

		filedStrs = append(filedStrs, tmpStr)
	}

	if this.IsServer{

		return this.SaveHashServer(structName, idVals, filedStrs, isCreate)

	}else{
		redRequest := new(RedDataRequest)
		redRequest.StructName = structName

		if isCreate{
			redRequest.Op = RedDataOpSave
		}else{
			redRequest.Op = RedDataOpUpdate
		}

		redRequest.IdVals = idVals
		redRequest.FieldVals = filedStrs

		retCh := make(chan interface{})
		this.sendRedFunc(redRequest, retCh)
		rspBytesInf, ok := <-retCh

		if ok == false {
			return 0, common.ErrorLog("rsp chan is closed")
		}

		buf, ok := rspBytesInf.(*common.MBuffer)
		if ok == false{
			return 0, common.ErrorLog("db rsp is not []byte")
		}

		rsp, err := this.handleRedBytesRspFunc(buf, this, model)
		if err  != nil{
			return 0, common.ErrorLog("handler bytes rsp failed, model:", structName, err)
		}

		savedId, ok := rsp.Data.(int64)
		if !ok{
			return 0, common.ErrorLog("saved id is not in rsp", structName)
		}

		return savedId, nil

	}

}


/*
func (this *XRedis) UpdateHash(objPtr interface{}, fields []string) error {

	objPtrVal := reflect.ValueOf(objPtr)

	if objPtrVal.Kind() != reflect.Ptr {

		return common.ErrorLog("objPtr is not ptr")
	}

	objVal := reflect.Indirect(objPtrVal)
	objType := objVal.Type()

	structName := objType.Name()

	model, err := getHashModelbyStructName(structName)
	if err != nil {
		return err
	}

	keyName := model.GetKeyNameFromValobj(this, objVal, false)

	ok, err := this.Conn.Exists(keyName).Result()
	if err !=  nil{
		return common.ErrorLog("get exist failed", keyName)
	}

	if ok == 0{
		return common.ErrorLog("not contains key", keyName)
	}

	valDic := make(map[string]interface{})
	for _, fieldName := range fields{
		if !model.IsValFieldName(fieldName){
			return common.ErrorLog("invalid model field name", fieldName)
		}

		valInf := objVal.FieldByName(fieldName).Interface()
		valDic[fieldName] = valInf
	}

	this.Conn.HMSet(keyName, valDic)

	return nil

}*/

func (this *XRedis) DeleteHashServer(structName string, idVals []int64) error{

	model, err := getHashModelbyStructName(structName)
	if err != nil{
		return err
	}

	keyName, err := model.GetKeyNameToIdxFromIdVals(idVals, len(model.IdFields))
	if err != nil{
		return err
	}

	_, idPart := RedKeySplit(keyName)

	for _, child := range model.Children{
		this.GcHashByModel(child, idPart)
	}

	if err := this.Conn.Del(keyName).Err(); err != nil{
		common.ErrorLog("del key err", keyName, err)
	}

	for _, cntIdx := range model.cntIdxs{

		prefix, idPart, err := RedKeySplitFree(keyName, cntIdx, 1)

		if err != nil{
			return common.ErrorLog("split free failed", keyName, cntIdx)
		}

		if model.cntType == RedCountTypeByNum{
			rest, err := this.Conn.Decr(prefix + "_Cnt").Result()
			if err != nil{
				return common.ErrorLog("decr cnt key failed", prefix + "_Cnt")
			}

			if rest <= 0{
				if err = this.Conn.Del(prefix + "_Cnt").Err(); err != nil{
					return common.ErrorLog("del cnt key failed", prefix + "_Cnt")
				}
			}
		}else{
			if err := this.Conn.SRem(prefix + "_Set", idPart).Err(); err != nil{
				return common.ErrorLog("del set cnt key failed", prefix + "_Set")
			}

			/*  srem will clear the set key if set is empty
			rest, err := this.Conn.SCard(prefix + "_Set").Result()

			if err != nil{
				return common.ErrorLog("del set cnt key failed", prefix + "_Set")
			}

			if rest <= 0{
				if err = this.Conn.Del(prefix + "_Set").Err(); err != nil{
					return common.ErrorLog("del set cnt key failed", prefix + "_Set")
				}
			}*/

		}
	}

	newInf := reflect.New(model.oriType)
	newInd := reflect.Indirect(newInf)

	for ii, idFieldName := range model.IdFields{
		newInd.FieldByName(idFieldName).SetInt(idVals[ii])
	}


	if model.delFunc != nil{
		if err := model.delFunc(model, newInf.Interface()); err != nil{
			common.ErrorLog("del func failed", keyName)
		}
	}

	return nil
}




func (this *XRedis) DeleteHash(objPtr interface{}) error {

	objPtrVal := reflect.ValueOf(objPtr)

	if objPtrVal.Kind() != reflect.Ptr {

		return common.ErrorLog("objPtr is not ptr")
	}

	objVal := reflect.Indirect(objPtrVal)
	objType := objVal.Type()

	structName := objType.Name()

	model, err := getHashModelbyStructName(structName)
	if err != nil {
		return err
	}

	idVals, err := model.GetIdVals(objVal, len(model.IdFields), false)
	if err != nil{
		return err
	}

	if this.IsServer{
		if err = this.DeleteHashServer(structName, idVals); err != nil{
			return common.ErrorLog("del hash server failed", structName, idVals)
		}

	}else{
		redRequest := new(RedDataRequest)
		redRequest.StructName = structName
		redRequest.Op = RedDataOpDel
		redRequest.IdVals = idVals

		retCh := make(chan interface{})
		this.sendRedFunc(redRequest, retCh)
		rspBytesInf, ok := <-retCh

		if ok == false {
			return common.ErrorLog("rsp chan is closed")
		}

		buf, ok := rspBytesInf.(*common.MBuffer)
		if ok == false{
			return common.ErrorLog("db rsp is not []byte")
		}

		rsp, err := this.handleRedBytesRspFunc(buf, this, model)
		if err  != nil{
			return common.ErrorLog("handler bytes rsp failed, model:", model.redNamePrefix, err)
		}

		delOk, ok := rsp.Data.(int64)
		if !ok{
			return common.ErrorLog("saved id is not in rsp", structName)
		}
		if delOk != 1{
			return common.ErrorLog("del is not ok", structName, idVals)
		}
	}

	return nil

}





func (this *XRedis) GcHashByModel(model *RedHashModel, idPart string) error{

	if len(model.IdFields) == 1{

		keyName := fmt.Sprintf("%s%s", model.redNamePrefix, idPart)
		if err := this.Conn.Del(keyName).Err(); err != nil{
			common.ErrorLog("del key err", keyName, err)
		}

		return nil
	}


	if model.cntType == RedCountTypeByNum && model.IsAutoIncr == true{

		autoIdxKeyName := model.redNamePrefix + idPart +  Idx_Ext_Str

		maxIdx, err := this.Conn.Get(autoIdxKeyName).Int64()

		if err != nil{
			common.ErrorLog("gc model idx empty", autoIdxKeyName)
			return nil
		}

		for ii := 1; ii <= int(maxIdx); ii++{

			keyName := fmt.Sprintf("%s%s_%d", model.redNamePrefix, idPart, ii)

			if err := this.Conn.Del(keyName).Err(); err != nil{
				common.ErrorLog("del key err", keyName, err)
				continue
			}

			if len(model.Children) > 0{

				for _, child := range model.Children{
					this.GcHashByModel(child, fmt.Sprintf("%s_%d", idPart, ii))
				}
			}
		}

		if err := this.Conn.Del(autoIdxKeyName).Err(); err != nil{
			common.ErrorLog("del key err", autoIdxKeyName, err)
		}

	}else if model.cntType == RedCountTypeBySet{

		herosKey := fmt.Sprintf("%s%s_Set", model.redNamePrefix, idPart)

		res, err := this.Conn.SMembers(herosKey).Result()
		if err != nil{
			common.ErrorLog("get set member failed", herosKey)
		}

		//redmodule.GetHashModelbyRedName(RedTableUserHeroStat)
		for _, idItem := range res{

			keyName := fmt.Sprintf("%s%s%s", model.redNamePrefix, idPart, idItem)

			if err := this.Conn.Del(keyName).Err(); err != nil{
				common.ErrorLog("del key err", keyName, err)
				continue
			}

			if len(model.Children) > 0{

				for _, child := range model.Children{
					this.GcHashByModel(child, fmt.Sprintf("%s%s", idPart, idItem))
				}
			}
		}

		if err := this.Conn.Del(herosKey).Err(); err != nil{
			common.ErrorLog("del set key err", herosKey, err)
		}


	}else{
		common.ErrorLog("the model cannot be count by num and not auto increase")
		return nil
	}

	return nil

}


func (this *XRedis) GcHash(rootKeyNamePart string, rootKeyIdPart string) error{   //rootkeypart length = idfield length -1

	model, err := GetHashModelbyRedName(rootKeyNamePart)
	if err != nil{
		return err
	}

	key := rootKeyNamePart + rootKeyIdPart

	for _, cntIdx := range model.cntIdxs{

		prefix, _, err := RedKeySplitFree(key, cntIdx, 1)

		if err != nil{
			return common.ErrorLog("split free failed", key, cntIdx)
		}

		if model.cntType == RedCountTypeByNum{
			if err := this.Conn.Del(prefix + "_Cnt").Err(); err != nil{
				return common.ErrorLog("del cnt key failed", prefix + "_Cnt")
			}
		}else{
			if err := this.Conn.Del(prefix + "_Set").Err(); err != nil{
				return common.ErrorLog("del set cnt key failed", prefix + "_Set")
			}


		}
	}

	return this.GcHashByModel(model, rootKeyIdPart)

}


















































