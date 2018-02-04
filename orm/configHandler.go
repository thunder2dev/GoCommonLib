package orm

import (
	"encoding/xml"
	"io/ioutil"
	"MarsXserver/common"
)


const(

	CLIENT_MODE = 1
	SERVER_MODE = 2

)


type  OrmConfigData struct{
	Mode		int			`xml:"mode"`
	DriverName      string			`xml:"db-name"`
	ConnectionInfo	string			`xml:"connect-info"`
}



func LoadConfig(name string) (configData *OrmConfigData, err error){

	configData = &OrmConfigData{}

	fileName := name + ".xml"

	content, err := ioutil.ReadFile(fileName)
	if err!= nil{
		common.ErrorLog("read config failed", err)
		return nil, err
	}

	if err = xml.Unmarshal(content, configData); err != nil{
		common.ErrorLog("unmarshal error", err)
		return nil, err
	}

	return configData, nil
}























