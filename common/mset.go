package common

import "sync"

type MSet struct{

	data map[interface{}]bool
	lock sync.RWMutex
}


func NewMSet(args... string) *MSet{

	set := &MSet{data: make(map[interface{}]bool)}

	for _, ele := range args{

		set.Insert(ele)

	}

	return set
}


func (this *MSet) Size() int{
	this.lock.RLock()
	defer  this.lock.RUnlock()
	return len(this.data)
}

func (this *MSet) IsEmpty() bool {

	this.lock.RLock()
	defer  this.lock.RUnlock()

	return len(this.data) > 0


}

func (this *MSet) Clear(){

	this.lock.Lock()
	defer this.lock.Unlock()

	for k := range this.data{
		delete(this.data, k)
	}

}


func (this *MSet) Insert(ele interface{}) bool{

	this.lock.Lock()
	defer this.lock.Unlock()

	if _, ok := this.data[ele]; ok == true{
		return false
	}

	this.data[ele] = true
	return true
}


func (this *MSet) Contains(ele interface{}) bool{

	this.lock.RLock()
	defer  this.lock.RUnlock()

	_, ok := this.data[ele]
	return ok


}


func (this *MSet) Remove(ele interface{}){
	this.lock.Lock()
	defer this.lock.Unlock()

	delete(this.data, ele)
}



func (this *MSet) ToList() []interface{}{
	this.lock.RLock()
	defer this.lock.RUnlock()

	keys := make([]interface{}, 0, len(this.data))

	for k := range this.data{
		keys = append(keys, k)
	}
	return keys
}








