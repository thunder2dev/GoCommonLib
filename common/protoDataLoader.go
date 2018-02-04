package common

import (
	"reflect"
	"path/filepath"
	"io/ioutil"
	"github.com/golang/protobuf/proto"
)

var(

	protoDataLoaders map[string]ProtoDataLoaderIterface = make(map[string]ProtoDataLoaderIterface)

)


const (

	ArrColumnName string = "Items"
	IdColumnName string = "Id"

)


type ProtoDataLoaderIterface interface{

	Init(name string, arrModelPtr interface{}, itemModelPtr interface{}) error

	Get(id int64) interface{}

	LoadData() error

	AfterLoad() error

}


type ProtoDataLoaderBase struct{

	Name string

	M_allData map[int64]interface{}

	ArrType reflect.Type
	ItemType reflect.Type



}


func (this *ProtoDataLoaderBase) Get(id int64) interface{}{

	res, _ := this.M_allData[id]

	return res
}

func (this *ProtoDataLoaderBase) Init(name string, arrModelPtr interface{}, itemModelPtr interface{}) error{

	this.Name = name

	arrVal := reflect.ValueOf(arrModelPtr)
	if arrVal.Kind() != reflect.Ptr{
		return ErrorLog("proto loader arr model must be ptr")
	}

	arrInd := reflect.Indirect(arrVal)

	itemVal := reflect.ValueOf(itemModelPtr)
	if itemVal.Kind() != reflect.Ptr{
		return ErrorLog("proto loader item model must be ptr")
	}

	itemInd := reflect.Indirect(itemVal)

	this.ArrType = arrInd.Type()
	this.ItemType = itemInd.Type()

	return nil
}



func RegisterProtoDataLoader(name string, loader ProtoDataLoaderIterface, arrModelPtr interface{}, itemModelPtr interface{}) error{

	loader.Init(name, arrModelPtr, itemModelPtr)

	if _, ok := protoDataLoaders[name]; ok == true{
		return ErrorLog("proto loader is loaded")
	}

	protoDataLoaders[name] = loader

	return nil
}

func GetProtoLoader(name string) ProtoDataLoaderIterface{  //notice no err

	loader, ok := protoDataLoaders[name]

	if !ok {
		ErrorLog("loader not registered", name)
	}

	return loader

}

func (this *ProtoDataLoaderBase) LoadData() error{

	this.M_allData = make(map[int64]interface{})

	fpath := filepath.Join(DefaultProtoBinPath, this.Name + ".data")
	if !IsFileExists(fpath){
		return ErrorLog("proto data file not existed")
	}

	fbytes, err := ioutil.ReadFile(fpath)
	if err != nil{
		return ErrorLog("read proto bin file failed", this.Name, err)
	}

	msgPtr := reflect.New(this.ArrType).Interface()
	message, check :=  msgPtr.(proto.Message)
	if check == false{
		return ErrorLog("cannot type to message:" + this.Name)
	}

	err = proto.Unmarshal(fbytes, message)
	if err != nil{
		return ErrorLog("unmarshal failed for msg:" + this.Name)
	}

	msgValobj := reflect.ValueOf(message)
	msgInd := reflect.Indirect(msgValobj)

	arrField := msgInd.FieldByName(ArrColumnName)

	if IsDefaultValueOfType(arrField){
		return ErrorLog("no arr column field" + this.Name)
	}

	if arrField.Kind() != reflect.Slice{
		return ErrorLog("no arr column is not array" + this.Name, arrField.Kind())
	}

	for ii := 0; ii < arrField.Len(); ii++{

		itemValobj := arrField.Index(ii)
		itemInd := reflect.Indirect(itemValobj)

		idField := itemInd.FieldByName(IdColumnName)

		if IsDefaultValueOfType(idField){
			return ErrorLog("id field is not existed" + this.Name)
		}

		idInd := reflect.Indirect(idField)

		itemId := idInd.Int()

		this.M_allData[itemId] = itemValobj.Interface()
	}


	return nil
}






func ProtoDataLoaderBootStrap(){


	for k, v := range protoDataLoaders{

		v.LoadData()
		v.AfterLoad()
		InfoLog("proto data ", k, " data loaded")

	}


}













