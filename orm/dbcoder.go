package orm

import (
	"MarsXserver/common"
	"reflect"
)

func OrmHandleBytesRspFunc(buf *common.MBuffer, expr *XOrmEpr) ([]interface{}, error){

	length, err := buf.ReadInt()
	if length < 0 || err != nil{
		return nil, common.ErrorLog("rsp is not a array, len", length, err)
	}

	if length == 0{
		return make([]interface{}, 0, 0), nil
	}

	colLen := len(expr.Data.RetNames)

	msg := &DBExprResponse{
	}

	val := reflect.ValueOf(msg)
	ind := reflect.Indirect(val)

	farr := ind.FieldByName("Data")
	arr := reflect.MakeSlice(reflect.TypeOf([]interface{}{}), length, length)

	if expr.Data.IsReturnFullMoel{

		for ii := 0; ii < length; ii++{

			newObj := reflect.New(expr.ModelInfo.OriType)
			newInd := reflect.Indirect(newObj)

			if err := common.DecodeObjectPacketValue(buf, newInd); err != nil{
				return nil, common.ErrorLog("decode db rsp arr item failed:", expr.ModelInfo.TableName, "index:", ii, err)
			}

			arr.Index(ii).Set(newObj)
		}

	}else if expr.Data.OpType == int(OP_SELECT){

		colType := make(map[string]DBDataType)

		for ii := 0; ii < colLen; ii++{
			fname := expr.Data.RetNames[ii]
			finfo := expr.ModelInfo.FieldDic[fname]
			colType[fname] = finfo.Ftype
		}

		for ii:=0; ii < length; ii++ {

			//common.InfoLog("ii:", ii, "buf start:", buf.Info())

			mapLen, err := buf.ReadInt()
			if err != nil{
				return nil, common.ErrorLog("map read len failed", expr.ModelInfo.TableName)
			}

			if mapLen != len(expr.Data.RetNames){
				return nil, common.ErrorLog("select rsp col nums not equal to retnames", expr.ModelInfo.TableName, " len:", length, expr.Data.RetNames )
			}

			dic := reflect.MakeMap(reflect.TypeOf(map[string]interface{}{}))
			for jj := 0; jj < mapLen; jj++ {
				keyStr, err := buf.ReadString()
				if err != nil {
					return nil, common.ErrorLog("read map key string failed", expr.ModelInfo.TableName)
				}

				key := reflect.New(reflect.TypeOf("")).Elem()
				key.SetString(keyStr)

				valStr, err := buf.ReadString()
				if err != nil{
					return nil, common.ErrorLog("read map val string failed", expr.ModelInfo.TableName)
				}

				//common.InfoLog("key", keyStr, "val:", valStr, "coltype:", colType[jj], " tps", colType)

				valInf, err := expr.GetValueFromStringForm(valStr, colType[keyStr])
				if err != nil{
					return nil, common.ErrorLog("get value from string form failed", err, expr.ModelInfo.TableName, expr.Data.RetNames[jj])
				}

				/*
				var val reflect.Value
				switch colType[jj]{
				case orm.TYPE_INT32:
					valInt, err := buf.ReadInt()
					if err != nil {
						return nil, common.ErrorLog("read map key int failed", expr.ModelInfo.TableName, "key:", keyStr)
					}
					val = reflect.New(reflect.TypeOf(int(0))).Elem()
					val.SetInt(int64(valInt))
				case orm.TYPE_INT64:
					valLong, err := buf.ReadLong()
					if err != nil {
						return nil, common.ErrorLog("read map key int64 failed", expr.ModelInfo.TableName, "key:", keyStr)
					}
					val = reflect.New(reflect.TypeOf(int64(0))).Elem()
					val.SetInt(valLong)
				default:
					valStr, err := buf.ReadString()
					if err != nil {
						return nil, common.ErrorLog("read map key string failed", expr.ModelInfo.TableName, "key:", keyStr)
					}
					val = reflect.New(reflect.TypeOf("")).Elem()
					val.SetString(valStr)
				}*/

				dic.SetMapIndex(key, reflect.ValueOf(valInf))
			}

			arr.Index(ii).Set(dic)
		}

	}else{

		intVal, err := buf.ReadInt()
		if err != nil{
			return nil, common.ErrorLog("read int failed", intVal)
		}
		if buf.Length() > 0{
			return nil, common.ErrorLog("there is more than one int in db rsp")
		}

		arr.Index(0).Set(reflect.ValueOf(intVal))
	}

	farr.Set(arr)

	return farr.Interface().([]interface{}), nil


}
