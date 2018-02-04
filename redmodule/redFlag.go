package redmodule

import (
	"fmt"
	"MarsXserver/common"
	"time"
	"strings"
	"reflect"
	"bytes"
	"strconv"
)



type RedFlagModel struct{

	redNamePrefix string
	allFields []string
	fieldTypes []RedValueType

	fieldNum int

	countkeyPatterns []string

	keyPattern string

	valType RedValueType    //can only be num or string

	collectIdxs []int

	timeGc *TimeGcModel

}


type RedFlagCollectRes struct {

	Res [][]interface{}
}


var (
	flagModels	map[string]*RedFlagModel
)


func init(){

	flagModels = make(map[string]*RedFlagModel)

}



func (this *RedFlagModel) GetTableType() RedTableType{
	return RedTableFlag
}

func (this *RedFlagModel) GetTableName() string{
	return this.redNamePrefix
}

func RegisterFlagModel(redPrefix string, objInf interface{}, fieldNames []string, valueType RedValueType, collectIdxs []int, gcDuration time.Duration) error{

	flagModel := &RedFlagModel{
		redNamePrefix: redPrefix,
		allFields: fieldNames,
		fieldTypes: make([]RedValueType, 0),
		fieldNum: len(fieldNames),
		countkeyPatterns: make([]string, 0),
		valType: valueType,
		collectIdxs: collectIdxs,
	}


	objVal := reflect.ValueOf(objInf)
	objInd := reflect.Indirect(objVal)


	keyPatBuff := new(bytes.Buffer)
	keyPatBuff.WriteString(fmt.Sprintf("%s", redPrefix))


	for ii:= 0; ii < flagModel.fieldNum; ii ++{

		fieldInfo := objInd.FieldByName(flagModel.allFields[ii])

		if !fieldInfo.IsValid(){
			return common.ErrorLog("no field", redPrefix, flagModel.allFields[ii])
		}

		switch fieldInfo.Kind() {
		case reflect.Int32, reflect.Int:
			flagModel.fieldTypes = append(flagModel.fieldTypes, RedValueTypeInt)
		case reflect.Int64:
			flagModel.fieldTypes = append(flagModel.fieldTypes, RedValueTypeInt64)
		case reflect.String:
			flagModel.fieldTypes = append(flagModel.fieldTypes, RedValueTypeString)
		}

		flagModel.countkeyPatterns = append(flagModel.countkeyPatterns, keyPatBuff.String())
		keyPatBuff.WriteString("_%v")

	}

	for _, collectIdx := range collectIdxs{

		if collectIdx + 1 > len(fieldNames){
			common.FatalLog("collectidx exceeds len", redPrefix, collectIdx)
		}
	}

	flagModel.keyPattern = keyPatBuff.String()

	flagModel.timeGc = RegiterTimeGcTable(flagModel, TimeGcNoUpdate, gcDuration)

	flagModels[redPrefix] = flagModel

	return nil
}


func GetFlagModelByName(name string) *RedFlagModel{

	model, ok := flagModels[name]
	if ok == false{
		return nil
	}
	return model
}


func checkFlagArgsType(args... interface{}) bool{

	for _, arg := range args{

		objVal := reflect.ValueOf(arg)
		switch objVal.Kind() {
		case reflect.Int, reflect.Int32, reflect.Int64, reflect.String:
			continue
		default:
			return false
		}
	}

	return true
}

func (this *XRedis) CollectFlagsByIdx(tbName string, collectIdx int, cursor, count int, vals... interface{})  (*RedFlagCollectRes, int, error){

	if !checkFlagArgsType(vals...){
		return nil, -1, common.ErrorLog("arguments is not int nor string")
	}

	model := GetFlagModelByName(tbName)
	if model == nil{
		return nil, -1, common.ErrorLog("flag model is null", tbName)
	}

	check := false

	for _, idx := range model.collectIdxs{
		if idx == collectIdx{
			check = true
			break
		}
	}

	if !check{
		return nil, -1, common.ErrorLog("not in collect idx", tbName, collectIdx)
	}

	if len(vals) != collectIdx{
		return nil, -1, common.ErrorLog("pars num not match idx", tbName, collectIdx, len(vals))
	}

	currKeyName := 	fmt.Sprintf(model.countkeyPatterns[collectIdx], vals...)

	keys, newCursor, err := this.Conn.SScan(currKeyName, uint64(cursor), "*", int64(count)).Result()
	if err != nil{
		return nil, -1, common.ErrorLog("sscan err", tbName, collectIdx, vals, cursor, count)
	}

	res := &RedFlagCollectRes{
		Res: make([][]interface{}, 0),
	}

	for _, key := range keys{

		this.IterateFlags(model, currKeyName + key, collectIdx, func(keyname string, collectDepth int){
			if collectDepth == len(model.collectIdxs) {
				keyParts := strings.Split(keyname, "_")
				if len(keyParts) != model.fieldNum + 1{
					common.ErrorLog("key parts dismatch collectIdx len", len(keyParts), len(model.collectIdxs))
					return
				}

				resRow := make([]interface{}, 0)
				for ii := collectIdx + 1; ii < len(keyParts); ii++{

					switch model.fieldTypes[ii - 1] {
					case RedValueTypeInt:
						num, err := strconv.Atoi(keyParts[ii])
						if err != nil{
							if len(keyParts) != len(model.collectIdxs) + 1 {
								common.ErrorLog("not num", keyname, ii)
								return
							}
						}
						resRow = append(resRow, num)

					case RedValueTypeString:
						resRow = append(resRow, keyParts[ii])   //todo
					}
				}

				val, err := this.Conn.Get(keyname).Int64()
				if err != nil{
					common.ErrorLog("get val failed", keyname)
					return
				}

				resRow = append(resRow, int(val))

				res.Res = append(res.Res, resRow)
			}
		})
	}

	return res, int(newCursor), nil

}

func (this *XRedis) SetFlag(tbName string, vals... interface{}) error{  //do not forget to give val

	if !checkFlagArgsType(vals...){
		return common.ErrorLog("arguments is not int nor string")
	}

	model := GetFlagModelByName(tbName)
	if model == nil{
		return common.ErrorLog("flag model is null", tbName)
	}

	if len(vals) != model.fieldNum + 1{
		return common.ErrorLog("args num err", len(vals), model.fieldNum)
	}

	key := 	fmt.Sprintf(model.keyPattern, vals[0:len(vals)-1]...)

	err := this.Conn.Set(key, vals[len(vals) -1], 0).Err()
	if err !=  nil{
		return common.ErrorLog("set flag failed", key, err)
	}

	var prevCollectIdx = model.fieldNum

	for ii := len(model.collectIdxs)-1; ii >=0; ii--{

		collectIdx := model.collectIdxs[ii]

		collectKeyName, lastIdPart, err := RedKeySplitFree(key, collectIdx, prevCollectIdx - collectIdx)
		if err != nil{
			return common.ErrorLog("split from end err", key, model.fieldNum - collectIdx, err)
		}

		prevCollectIdx = collectIdx

		//if !this.Conn.SIsMember(collectKeyName, lastIdPart).Val(){

		if ii == 0 {
			model.timeGc.AddTimeGcKey(this ,collectKeyName)
		}

		if err := this.Conn.SAdd(collectKeyName, lastIdPart).Err(); err != nil{
			return common.ErrorLog("add to collect key failed", collectKeyName, lastIdPart)
		}

	}

	return nil
}




func (this *XRedis) ContainsFlag(tbName string, vals... interface{}) (bool, error){

	if !checkFlagArgsType(vals...){
		return false, common.ErrorLog("arguments is not int nor string")
	}

	model := GetFlagModelByName(tbName)
	if model == nil{
		return false, common.ErrorLog("flag model is null", tbName)
	}

	if len(vals) != model.fieldNum{
		return false, common.ErrorLog("args num err", tbName ,len(vals), model.fieldNum)
	}

	key := 	fmt.Sprintf(model.keyPattern, vals...)

	ok, err := this.Conn.Exists(key).Result()
	if err !=  nil{
		return false, common.ErrorLog("get flag failed", key, err)
	}

	return ok == 1, nil
}



func (this *XRedis) GcFlag(keyPrefix, idPart string) error{

	model := GetFlagModelByName(keyPrefix)
	if model == nil{
		return common.ErrorLog("no flag model", keyPrefix, idPart)
	}

	return this.IterateFlags(model, keyPrefix + idPart, 1, func(key string, collectDepth int){
		this.Conn.Del(key)
	})

}



func (this *XRedis) IterateFlags(model *RedFlagModel, key string, collectDepth int, handler func(key string, collectDepth int)) error{ // first call collectDepth=len(model.collectIdxs)

	if collectDepth >= len(model.collectIdxs){
		handler(key, collectDepth)
		return nil
	}

	idParts, err := this.Conn.SMembers(key).Result()
	if err != nil{
		return common.ErrorLog("get members failed", key, err)
	}

	for _, idPart := range idParts{

		lowerKey := key + idPart

		this.IterateFlags(model, lowerKey, collectDepth +1, handler)

	}

	handler(key, collectDepth)

	return nil
}


/*
func (this *XRedis) FlagGC(now time.Time){


	for{
		common.InfoLog("redis gc once")
		ele, err := this.Conn.LIndex(Flag_Tm_List_Name, 0).Result()
		if err != nil || len(ele) <= 0{
			common.InfoLog("gc failed, time list empty", err)
			return
		}

		eleArr := strings.Split(ele, ",")
		key := eleArr[0]
		tmStr := eleArr[1]

		tm, err := time.Parse("2006010215", tmStr)
		if err != nil{
			common.ErrorLog("tm is not formatted", tmStr)
			return
		}

		if now.Sub(tm) > Default_Gc_Duration{
			_, err := this.Conn.LPop(Flag_Tm_List_Name).Result()
			if err != nil{
				common.ErrorLog("gc pop failed", )
				return
			}

			err = this.Conn.Del(key).Err()
			if err != nil{
				common.ErrorLog("gc del key failed", key)
				return
			}

		}else{
			break
		}

	}


}
*/










