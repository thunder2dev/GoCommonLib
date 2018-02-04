package httpserver



type HttpConfigData struct{

	Port	int		`xml:"port"`
	Sid	int		`xml:"sid"`
	TemplatePath string	`xml:"template-path"`
	StaticPaths HttpStaticPaths	`xml:"static-paths"`
}

type HttpStaticPaths struct{
	Paths 	[]HttpStaticPath	`xml:"path"`

}

type HttpStaticPath struct{
	Src 	string 		`xml:"src"`
	Dst	string		`xml:"dst"`
}


/*
func LoadConfig(name string) (configData *HttpConfigData, err error){

	configData = &HttpConfigData{}

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
}*/

























