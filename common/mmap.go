package common

import "sync"

type MMap struct{
	lock *sync.RWMutex
	dic map[interface{}]interface{}
}


func NewMMap() *MMap{

	return &MMap{
		lock: new(sync.RWMutex),
		dic: make(map[interface{}]interface{}),
	}

}

func (m *MMap) Keys() []interface{}{

	m.lock.RLock()
	defer  m.lock.RUnlock()

	keys := make([]interface{}, 0, len(m.dic))

	for k := range m.dic{
		keys = append(keys, k)
	}

	return keys

}

//Get from maps return the k's value
func (m *MMap) Get(k interface{}) interface{} {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if val, ok := m.dic[k]; ok {
		return val
	}
	return nil
}

func (m *MMap) Set(k interface{}, v interface{}) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if val, ok := m.dic[k]; !ok {
		m.dic[k] = v
	} else if val != v {
		m.dic[k] = v
	} else {
		return false
	}
	return true
}

// Returns true if k is exist in the map.
func (m *MMap) Contains(k interface{}) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if _, ok := m.dic[k]; !ok {
		return false
	}
	return true
}

func (m *MMap) Delete(k interface{}) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.dic, k)
}


func (m *MMap) Count() int{
	m.lock.RLock()
	defer m.lock.RUnlock()

	return len(m.dic)

}

















