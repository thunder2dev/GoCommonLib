package redmodule



import (
	"MarsXserver/common"
	"reflect"
)

func RedHandleBytesRspFunc(buf *common.MBuffer, red *XRedis, model *RedHashModel) (*RedDataResponse, error){


	msg := &RedDataResponse{
	}

	val := reflect.ValueOf(msg)
	ind := reflect.Indirect(val)

	dataField := ind.FieldByName("Data")

	opCode, err := buf.ReadInt()
	if opCode <= 0 || opCode >= int(RedDataOpMax) || err != nil{
		return nil, common.ErrorLog("rsp op err", opCode, err)
	}

	msg.Op = RedDataOpType(opCode)

	switch RedDataOpType(opCode) {
	case RedDataOpUpdate, RedDataOpSave, RedDataOpCnt, RedDataOpDel  :
		opId, err := buf.ReadLong()
		if err != nil{
			return nil, common.ErrorLog("read saved id err", opCode, err, model.redNamePrefix)
		}
		dataField.Set(reflect.ValueOf(opId))
	case RedDataOpRead:

		len, err := buf.ReadInt()
		if len < 0 || len > common.DefaultRedArrResponseLimit{
			return nil, common.ErrorLog("rsp arr len err:", len, err)
		}

		arr := reflect.MakeSlice(reflect.TypeOf([]interface{}{}), len, len)

		for ii:=0; ii < len; ii++{
			newObj := reflect.New(model.oriType)
			newInd := reflect.Indirect(newObj)
			if err := common.DecodeObjectPacketValue(buf, newInd); err != nil{
				return nil, common.ErrorLog("decode red item failed:", model.redNamePrefix, err)
			}
			arr.Index(ii).Set(newObj)
		}

		dataField.Set(arr)

	default:
		return nil, common.ErrorLog("red op case not included:", opCode, model.redNamePrefix)
	}




	return msg, nil


}




