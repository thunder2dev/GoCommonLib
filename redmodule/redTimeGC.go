package redmodule

import (
	"time"
	"fmt"
	"MarsXserver/common"
	"strings"
	"github.com/go-redis/redis"
)

const (

	RedTimeIndexStaticPrefix = "TGCs"
	RedTimeIndexStaticSetPrefix = "TGCe"
	RedTimeIndexDynamicPrefix = "TGCd"
)

type TimeGcUpdateType int

const(

	_  TimeGcUpdateType = iota
	TimeGcNoUpdate
	TimeGcDynamicUpdate

)


type TimeGcModel struct {
	Model RedModel
	UpdateType TimeGcUpdateType
	Duration time.Duration
}

var (
	timeGcModels map[string]*TimeGcModel
)

func init() {
	timeGcModels = make(map[string]*TimeGcModel)
}


func RegiterTimeGcTable(serveModel RedModel, updateType TimeGcUpdateType, duration time.Duration) *TimeGcModel{

	timeGcQ := &TimeGcModel{
		Model:serveModel,
		UpdateType: updateType,
		Duration: duration,
	}

	timeGcModels[serveModel.GetTableName()] = timeGcQ

	return timeGcQ
}


func GetTimeGcModel(tbName string) *TimeGcModel{

	model, ok := timeGcModels[tbName]
	if ok == false{
		return nil
	}
	return model
}


func (this *TimeGcModel)GetTimeGcKey(updateType TimeGcUpdateType) string{
	if updateType == TimeGcDynamicUpdate{
		return fmt.Sprintf("%s_%s", RedTimeIndexDynamicPrefix, this.Model.GetTableName())
	}else{
		return fmt.Sprintf("%s_%s", RedTimeIndexStaticPrefix, this.Model.GetTableName())
	}
}


func (this *TimeGcModel) Exists(red *XRedis, updateType TimeGcUpdateType, idPart string) bool{

	if updateType == TimeGcDynamicUpdate{

		key := this.GetTimeGcKey(updateType)

		return red.Conn.ZScore(key, idPart).Err() != nil
	}else{

		key := fmt.Sprintf("%s_%s", RedTimeIndexStaticSetPrefix, this.Model.GetTableName())

		return red.Conn.SIsMember(key, idPart).Val()

	}
}



func (this *TimeGcModel)AddTimeGcKey(red *XRedis, key string) error{

	model := GetTimeGcModel(this.Model.GetTableName())
	if model == nil{
		return common.ErrorLog("no model for", this.Model.GetTableName())
	}

	now := common.GetTimeNow()

	nowStr := now.Format("20060102150405")

	tidxKey := this.GetTimeGcKey(model.UpdateType)

	_, idPart := RedKeySplit(key)

	if this.Exists(red, model.UpdateType, idPart){
		return nil
	}

	setKey := fmt.Sprintf("%s_%s", RedTimeIndexStaticSetPrefix, this.Model.GetTableName())
	if err := red.Conn.SAdd(setKey, idPart).Err(); err != nil{
		common.ErrorLog("time gc set err", setKey, idPart, err)
	}

	if model.UpdateType == TimeGcNoUpdate{

		return red.Conn.RPush(tidxKey, fmt.Sprintf("%s,%s", idPart, nowStr)).Err()
	}else{
		return red.Conn.ZAdd(tidxKey, redis.Z{ Score:float64(now.Unix()), Member: idPart}).Err()
	}
}


func (red *XRedis)TimeGCAll(){

	for _, model := range timeGcModels{
		TimeGCOneModel(red, model)
	}

}


func TimeGCOneModel(red *XRedis, model *TimeGcModel) error{

	now := common.GetTimeNow()

	timeEnd := now.Add(-model.Duration)

	switch model.UpdateType {
	case TimeGcNoUpdate:
		return TimeGCStaticModel(red, model, timeEnd)
	case TimeGcDynamicUpdate:
		return TimeGCDynamicModel(red, model, timeEnd)
	default:
		return common.ErrorLog("model update type err", model.Model.GetTableName(), model.UpdateType)
	}

	return nil
}




func TimeGCDynamicModel(red *XRedis, model *TimeGcModel, timeEnd time.Time) error{

	tidxKey := model.GetTimeGcKey(model.UpdateType)

	timeEndUnix := timeEnd.Unix()

	timeEndStr := fmt.Sprintf("(%d", timeEndUnix)

	keyIdParts := make([]string, 0)

	if err := red.Conn.ZRangeByScore(tidxKey, redis.ZRangeBy{Min: "-inf", Max: timeEndStr}).ScanSlice(&keyIdParts); err != nil{
		return common.ErrorLog(fmt.Sprintf("zrange time idx failed, endTime:%s", timeEndStr), err)
	}

	if err := red.Conn.ZRemRangeByScore(tidxKey, "-inf", timeEndStr); err != nil{
		return common.ErrorLog("time gc dynamic err", tidxKey, timeEndStr, err)
	}

	for _, keyIdPart := range keyIdParts {

		if err := TimeGCItem(red, model, keyIdPart); err != nil{
			return common.ErrorLog("gc dynamic time item err", err)
		}

	}

	return nil
}




func TimeGCStaticModel(red *XRedis, gcModel *TimeGcModel, timeEnd time.Time) error{

	tidxKey := gcModel.GetTimeGcKey(gcModel.UpdateType)

	for{
		common.InfoLog("time gc for", gcModel.Model.GetTableName())
		ele, err := red.Conn.LIndex(tidxKey, 0).Result()
		if err != nil || len(ele) <= 0{
			common.InfoLog("gc failed, time list empty", gcModel.Model.GetTableName(), err)
			return nil
		}

		eleArr := strings.Split(ele, ",")
		keyIdPart := eleArr[0]
		//key := fmt.Sprintf("%s%s", gcModel.Model.GetTableName(), keyIdPart)
		tmStr := eleArr[1]

		tm, err := time.Parse("20060102150405", tmStr)
		if err != nil{
			common.ErrorLog("tm is not formatted", tmStr)
			return nil
		}

		common.InfoLog("time sub", timeEnd.Sub(tm))
		if timeEnd.Sub(tm) > 0{

			if err := red.Conn.LPop(tidxKey).Err(); err != nil{
				return common.ErrorLog("time gc pop err", gcModel.Model.GetTableName(), err)
			}

			if err := TimeGCItem(red, gcModel, keyIdPart); err != nil{
				return common.ErrorLog("gc static time item err", err)
			}

			setkey := fmt.Sprintf("%s_%s", RedTimeIndexStaticSetPrefix, gcModel.Model.GetTableName())

			if err := red.Conn.SRem(setkey, keyIdPart).Err(); err != nil{
				common.InfoLog("remove from time set err", setkey, keyIdPart)
			}

		}else{
			break
		}




	}

	return nil

}




func TimeGCItem(red *XRedis, gcModel *TimeGcModel, idPart string) error{

	switch gcModel.Model.GetTableType() {
	case RedTableHash:
		if err := red.GcHash(gcModel.Model.GetTableName(), idPart); err != nil{
			common.ErrorLog("gc hash failed", gcModel.Model.GetTableName(), idPart)
			return nil
		}
	case RedTableFlag:
		if err := red.GcFlag(gcModel.Model.GetTableName(), idPart); err != nil{
			common.ErrorLog("gc flag failed", gcModel.Model.GetTableName(), idPart)
			return nil
		}
	default:
		return common.ErrorLog("unkown model type", gcModel.Model.GetTableName(), idPart)
	}

	return nil
}

























