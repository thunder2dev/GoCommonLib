package tcpserver

import (
	"encoding/xml"
	"io/ioutil"
	"MarsXserver/common"
)


type TcpConfigData struct{

	Sid       int			`xml:"sid"`
	ServerList TcpServerItems       `xml:"servers"`
	Dialers	TcpDialerItems		`xml:"dialers"`
}

type TcpDialerItems struct{
	Dialer	[]int		`xml:"dialer"`
}


type TcpServerItems struct{
	Servers []TcpServerConfigItem `xml:"server"`
}


type TcpServerConfigItem struct{
	Sid   int		`xml:"sid"`
	SType string	`xml:"type"`
	Ip    string	`xml:"ip"`
	Port  int	`xml:"port"`
}


func LoadConfig(name string) (configData *TcpConfigData, err error){

	configData = &TcpConfigData{}

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


func (this *TcpConfigData) GetCurrServerItem() (item *TcpServerConfigItem, err error){

	serverItem, err := this.GetServerItemById(this.Sid)
	if err != nil{
		common.ErrorLog("curr tcpserver sid is not correct", this.Sid)
		return nil, err
	}
	return serverItem, nil

}

func (this *TcpConfigData) GetServerItemById(sid int) (item *TcpServerConfigItem, err error){

	for _, svr := range this.ServerList.Servers {
		if svr.Sid == sid{

			return &svr, nil
		}

	}

	return nil, common.ErrorLog("tcpserver sid is not correct", this.Sid)
}

























