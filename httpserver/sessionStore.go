package httpserver

import (
	"time"
	"MarsXserver/common"
)

type SessionStore struct{
	sid string

	lastAccessedTime time.Time

	data map[string]interface{}
}


func NewSession(_sid string) *SessionStore{

	return &SessionStore{
		sid: _sid,
		lastAccessedTime: common.GetTimeNow(),
		data : make(map[string]interface{}),
	}

}


func (this *SessionStore) Set(key string, value interface{}){

	this.data[key] = value

}

func (this *SessionStore) Get(key string) interface{}{

	value, ok := this.data[key]
	if ok {
		return value
	}

	return nil
}

func (this *SessionStore) Delete(key string) {

	delete(this.data, key)

}