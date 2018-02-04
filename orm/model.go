package orm

import (
	"reflect"
	"time"
	"errors"
	"strings"
	"strconv"
	"MarsXserver/common"
	"fmt"
)

type DBDataType int

const (
	_ 	DBDataType = iota
	TYPE_INT32
	TYPE_INT64
	TYPE_STRING
	TYPE_TIME
	TYPE_REL
	TYPE_UNKOWN_PTR

)


const (
	MAX_COLS_NUM = 20
)


type DBfieldInfo struct{
	oriPtrValue reflect.Value
	oriType     reflect.Type
	Ftype       DBDataType
	name        string
	idx         int

	size int
	isPk bool
	isAuto bool
	isDefault bool

	relName string
	relModel *DBModelInfo
}



type DBModelInfo struct{

	TableName string

	oriPtrValue reflect.Value
	OriType     reflect.Type
	fieldNames  []string
	fields      []*DBfieldInfo
	FieldDic    map[string]*DBfieldInfo

	pkField *DBfieldInfo

	fks map[string]*DBModelInfo
}


func getDBStyleName(str string) string{

	subStr := make([]string, 0, 20)

	ii, jj:= 0, 0
	for ii= 0; ii < len(str); ii++ {

		if str[ii] > 'A' && str[ii] < 'Z' && ii > 0{
			subStr = append(subStr, str[jj:ii])
			jj = ii
		}

	}

	if ii > jj{
		subStr = append(subStr, str[jj:ii])
	}

	res := strings.Join(subStr, "_")

	res = strings.ToLower(res)

	return res
}



func (model *DBModelInfo) getColumnType(v reflect.Value) (tp DBDataType, err error){

	//common.DebugLog("field kind:", v.Kind())

	switch v.Kind() {
	case reflect.String:
		tp = TYPE_STRING
	case reflect.Int64:
		tp = TYPE_INT64
	case reflect.Int32:
		tp = TYPE_INT32
	case reflect.TypeOf(time.Time{}).Kind():
		tp = TYPE_TIME
	case reflect.Ptr:
		tp = TYPE_UNKOWN_PTR
	default:
		common.ErrorLog("col type not supported:", v.Kind())
		err = errors.New("col type not supported")
	}

	return
}



func getStringColumnValue(str string) string{
	return fmt.Sprintf("%s", str)
}

func getTimeColumnValue(tm time.Time) string{
	return tm.Format("2006-01-02 15:04:05")
}


func (this *XOrmEpr) GetValueFromStringForm(str string, ftype DBDataType) (interface{}, error){

	switch ftype {
	case TYPE_INT32:
		if len(str) == 0{
			return int32(0), nil
		}
		intVal, err := strconv.ParseInt(str, 10, 32)
		if err != nil{
			return nil, err
		}
		return int32(intVal), nil
	case TYPE_INT64:
		if len(str) == 0{
			return int64(0), nil
		}
		return strconv.ParseInt(str, 10, 64)
	case TYPE_STRING:
		return str,nil
	case TYPE_TIME:
		if len(str) == 0{
			return nil, common.ErrorLog("time col cannot be empty string")
		}

		return common.FromDBTimeString(str)
	}

	return nil, common.ErrorLog("get val from string shouldn't get here")
}


func (this *XOrmEpr) getColumnStringValue(v reflect.Value, ftype DBDataType) (res string, err error){

	var ok bool = true
	switch ftype {
	case TYPE_INT32:
		val32, ok := v.Interface().(int32)
		if !ok{
			goto getColumnValue
		}
		val64 := int64(val32)
		res = strconv.FormatInt(val64, 10)
	case TYPE_INT64:
		val64, ok := v.Interface().(int64)
		if !ok{
			goto getColumnValue
		}
		res = strconv.FormatInt(val64, 10)
	case TYPE_STRING:
		str, ok := v.Interface().(string)
		if !ok{
			goto getColumnValue
		}
		res = getStringColumnValue(str)
	case TYPE_TIME:
		tm, ok := v.Interface().(time.Time)
		if !ok{
			goto getColumnValue
		}
		res = getTimeColumnValue(tm)
	case TYPE_REL:
		relName := getDBStyleName(v.Type().Name())
		relModal, err := this.Orm.getModel(relName)
		if err != nil{
			ok = false
			goto getColumnValue
		}

		relInd := reflect.Indirect(v)

		relId32, err := relModal.getPkValue(relInd)
		if err != nil{
			ok = false
			goto getColumnValue
		}
		res = strconv.FormatInt(int64(relId32), 10)


	default:
		return "", common.ErrorLog("type id is wrong:", ftype)
	}



getColumnValue:
	if ok == false{
		return "", common.ErrorLog("get col value failed, required:", ftype, " indeed:", res)
	}

	return

}


func (model *DBModelInfo) tagDecorate(dbField *DBfieldInfo, _tag string) error{

	tags := strings.Split(_tag, ",")

	for _, tag := range tags{
		switch tag {
		case "auto":
			dbField.isAuto = true
			dbField.isPk = true
		case "pk":
			dbField.isPk = true
		case "default":
			dbField.isDefault = true
		case "rel":

			if dbField.Ftype != TYPE_UNKOWN_PTR{
				return common.ErrorLog("rel is mistakingly used(field is not pointer")
			}
			dbField.Ftype = TYPE_REL
			dbField.name = getDBStyleName(dbField.name + "Id")
		}


		if strings.HasPrefix(tag, "size"){

			if dbField.Ftype != TYPE_STRING{
				return common.ErrorLog("tag size is mistakingly used(field is not string")
			}

			sizeStr := tag[len("size("): len(tag) - 1]
			size, err := strconv.ParseInt(sizeStr, 10, 32)
			if err != nil{
				return common.ErrorLog("size str is not num:", sizeStr, err)
			}
			dbField.size = int(size)
		}
	}

	return nil
}




func (model *DBModelInfo) initFields(v reflect.Value) error{

	for i:= 0; i < v.NumField(); i++{
		typefield := v.Type().Field(i)
		fieldName := getDBStyleName(typefield.Name)

		fieldType, err := model.getColumnType(v.Field(i))
		if err != nil{
			return err
		}

		finfo := &DBfieldInfo{
			oriPtrValue: v.Field(i),
			name:        fieldName,
			Ftype:       fieldType,
			idx:         i,
		}

		tag := typefield.Tag.Get("orm")

		if err = model.tagDecorate(finfo, tag); err != nil{
			return err
		}

		fieldName = finfo.name

		if finfo.Ftype == TYPE_REL{
			relTypeName := v.Type().Field(i).Type.Elem().Name()
			common.DebugLog(relTypeName)
			finfo.relName = relTypeName
		}

		model.FieldDic[fieldName] = finfo
		model.fields = append(model.fields, finfo)
		model.fieldNames = append(model.fieldNames, fieldName)
	}


	return nil

}



func (this *XOrmEpr) getInputValues(data interface{}, isSelect bool) ( colNames, colVals []string, err error) {

	val := reflect.ValueOf(data)
	ind := reflect.Indirect(val)

	colsIterNames := make([]string, 0, MAX_COLS_NUM)

	if isSelect {
		colsIterNames = append(colsIterNames, this.ModelInfo.pkField.name)
	}else {
		for _, fName := range this.ModelInfo.fieldNames{
			colsIterNames = append(colsIterNames, fName)
		}
	}

	colNames = make([]string, 0, len(colsIterNames))
	colVals = make([]string, 0, len(colsIterNames))

	for _, colName := range colsIterNames {

		finfo,ok := this.ModelInfo.FieldDic[colName]
		if ok != true{
			return nil, nil, common.ErrorLog("model:", this.ModelInfo.TableName, " has no field name:", finfo.name)
		}

		if finfo.idx >= ind.NumField() || finfo.idx < 0{
			return nil, nil, common.ErrorLog("model:", this.ModelInfo.TableName, " field index Limit:", finfo.name, " idx:", finfo.idx)
		}

		fieldValue := ind.Field(finfo.idx)

		if fieldValue.Type().Kind() == reflect.Ptr && common.IsDefaultValueOfType(fieldValue.Interface()){
			continue
		}


		fieldValueInd := reflect.Indirect(fieldValue)


		if common.IsDefaultValueOfType(fieldValueInd.Interface()){
			continue //for debug todo
			if finfo.Ftype == TYPE_REL || finfo.isAuto || finfo.isDefault{
				continue
			}/*else{
				return nil, nil, common.ErrorLog("field is not assigned:", finfo.name, " value:", fieldValueInd)
			}*/
		}

		dbValue, err := this.getColumnStringValue(fieldValueInd, finfo.Ftype)
		if err != nil{
			return nil, nil, common.ErrorLog("get db value failed")
		}

		colNames = append(colNames, colName)
		colVals = append(colVals, dbValue)

	}

	return

}


func (model *DBModelInfo)setPkValue(ind reflect.Value, pkId int32) error{

	if model.pkField == nil{
		return common.ErrorLog("pk field is nil, tb:", model.TableName)
	}

	if model.pkField.idx < 0 || model.pkField.idx > ind.NumField(){

		return common.ErrorLog("pk idx error, tb:", model.TableName, " idx:", model.pkField.idx)
	}

	pkFinfo := ind.Field(model.pkField.idx)
	pkFinfo.Set(reflect.ValueOf(pkId))

	return nil
}

func (model *DBModelInfo)getPkValue(ind reflect.Value) (pkId int32, err error){

	if model.pkField == nil{
		return 0, common.ErrorLog("pk field is nil, tb:", model.TableName)
	}

	if model.pkField.idx < 0 || model.pkField.idx > ind.NumField(){
		return 0, common.ErrorLog("pk idx error, tb:", model.TableName, " idx:", model.pkField.idx)
	}

	pkFinfo := ind.Field(model.pkField.idx)
	pkId, ok := pkFinfo.Interface().(int32)
	if ok == false{
		return 0, common.ErrorLog("pk val is not int32, tb:", model.TableName, " val:", pkFinfo.Interface())
	}

	return pkId, nil
}



func (this *XOrmEpr)createNewFromScanArgs(scanArgs []interface{}) (data reflect.Value, err error){

	ind := reflect.Indirect(this.ModelInfo.oriPtrValue)
	newModel := reflect.New(ind.Type())
	newInd := reflect.Indirect(newModel)

	for ii, finfo := range this.ModelInfo.fields{

		newField := newInd.Field(finfo.idx)
		arg := scanArgs[ii]
		argInterface := reflect.Indirect(reflect.ValueOf(arg)).Interface()
		//argInd := argInterface.
		//common.InfoLog("arg kind", argInterface)

		if finfo.Ftype == TYPE_REL{

			if finfo.relModel == nil{
				return reflect.Value{}, common.ErrorLog("rel modal is nil:", finfo.name, " relname:", finfo.relName)
			}

			var relId int32

			switch argInterface.(type) {
			case int32:
				relId = argInterface.(int32)
			case int64:
				relId = int32(argInterface.(int64))
			default:
				relId = int32(0)
				//todo return reflect.Value{},common.ErrorLog("rel id cannot be read:", argInterface)
			}

			mInd := reflect.Indirect(finfo.relModel.oriPtrValue)

			mNew := reflect.New(mInd.Type())
			mNewInd := reflect.Indirect(mNew)
			mNewInterface := mNew.Interface()
			//mNewFieldInd := reflect.Indirect(newField)

			if err := finfo.relModel.setPkValue(mNewInd, relId); err != nil{
				return reflect.Value{},err
			}

			if relId > 0{
				this.Orm.Read(mNewInterface, true)
			}

			newField.Set(mNew)


		}else if err := this.newDataFieldSet(finfo, newField, argInterface); err != nil{
			return reflect.Value{}, err
		}

	}

	return newInd, nil

}


func (this *XOrmEpr) newDataFieldSet(finfo *DBfieldInfo, newFieldValue reflect.Value, arg interface{}) error{

	argValue := reflect.ValueOf(arg)

	switch finfo.Ftype {
	case TYPE_INT32:

		switch innerVal := arg.(type) {
		case int32:
			newFieldValue.Set(argValue)
		case int64:
			newFieldValue.Set(reflect.ValueOf(int32(innerVal)))
		default:
			if arg == nil{
				newFieldValue.SetInt(0)
				return nil
			}//todo
			return common.ErrorLog("field set failed:", finfo.Ftype, " arg:", arg)
		}
	case TYPE_INT64:

		switch innerVal := arg.(type) {
		case int32:
			newFieldValue.Set(reflect.ValueOf(int64(innerVal)))
		case int64:
			newFieldValue.Set(argValue)
		default:
			if arg == nil{
				newFieldValue.SetInt(0)
				return nil
			}//todo
			return common.ErrorLog("field set failed:", finfo.Ftype, " arg:", arg)
		}
	case TYPE_STRING:
		str, ok := arg.(string)
		if ok != true{
			if arg == nil{
				newFieldValue.SetString("")
				return nil
			}
			return common.ErrorLog("arg is not string:", arg)
		}
		newFieldValue.SetString(str)
	case TYPE_TIME:

		if arg == nil{
			newFieldValue.Set(reflect.ValueOf(common.GetTimeNow()))
			return nil
		}

		switch argVal := arg.(type) {
		case string:
			tm, err := this.Orm.driver.fromTimeStr(argVal)
			if err != nil{
				return err
			}
			newFieldValue.Set(reflect.ValueOf(tm))
		case time.Time:
			newFieldValue.Set(reflect.ValueOf(argVal))
		default:
			return common.ErrorLog("encode time field with unkown type", arg)
		
		}
	case TYPE_REL:

		if finfo.relModel == nil{
			return common.ErrorLog("relmodel null")
		}

		if finfo.relModel.pkField == nil{
			return common.ErrorLog("pk field nil")
		}

		relPkField := finfo.relModel.pkField

		valRel := reflect.New(finfo.relModel.oriPtrValue.Type())
		newFieldValue.Set(valRel)
		valRelInd := reflect.Indirect(valRel)
		valRelPkFieldValue := valRelInd.Field(relPkField.idx)

		this.newDataFieldSet(relPkField, valRelPkFieldValue, arg)

	default:
		return common.ErrorLog("Ftype is not exists:", finfo.Ftype)
	}

	return nil

}





























