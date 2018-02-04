package redmodule



import (
	"strings"
)

type RedisConfigData struct{
	Addr		string 		`xml:"addr"`
	Password	string		`xml:"pass"`
}



func (this *RedisConfigData) Trim(){
	this.Addr = strings.TrimSpace(this.Addr)
	this.Password = strings.TrimSpace(this.Password)
}
























