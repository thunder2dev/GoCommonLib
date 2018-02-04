package orm

import (
	"sync"
	"errors"
	"time"
	"database/sql"
	"MarsXserver/common"
)

type OrmDriver interface{

	open(dbUrl string) error
	close()

	fromTimeStr(tmStr string) (tm time.Time, err error)

	Exists(modelInfo *DBModelInfo) (ok bool, err error)
	Create(modelInfo *DBModelInfo) error
	Insert(modelInfo *DBModelInfo, colNames, colVals []string) (idNum int32, err error)
	Select(ormExpr *XOrmEpr, colNames, colVals, retNames, filters []string, pageClause string, orderClause string) (resArr []interface{}, err error)
	Update(modelInfo *DBModelInfo, colNames, colVals []string) (int, error)
	Delete(modelInfo *DBModelInfo, colNames, colVals []string) (int, error)
	Count(modelInfo *DBModelInfo) (count int, err error)
	AddIndex(modelInfo *DBModelInfo, colName string) error


}



type OrmDriverBase struct{

	user string
	password string
	dbName string

	db *sql.DB
}


type OrmDriverMgr struct {

	drivers map[string]OrmDriver
	lock sync.RWMutex
}

var (
	ormDrivers *OrmDriverMgr = &OrmDriverMgr{
		drivers: make(map[string]OrmDriver),
	}

)




func RegisterOrmDriver(name string, driver OrmDriver) error{

	ormDrivers.lock.Lock()
	defer ormDrivers.lock.Unlock()

	if _, ok := ormDrivers.drivers[name]; ok == true{

		common.ErrorLog("orverlapped driver")
		return errors.New("orverlapped driver")

	}

	ormDrivers.drivers[name] = driver

	return nil
}
