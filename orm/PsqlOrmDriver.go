package orm

import (
	"time"
	"fmt"
	"strings"
	"bytes"
	"database/sql"
	_ "github.com/lib/pq"
	"MarsXserver/common"
)

var psqlTagNames = map[string]string{

	"auto":		"serial NOT NULL PRIMARY KEY",
	"pk":		"NOT NULL PRIMARY KEY",
	"bool":		"bool",
	"string":	"varchar(%d)",
	"time.date":	"date",
	"time.time":	"timestamp with time zone",
	"int32":	"integer",
	"int64":	"bigint",
	"float64":	"double precision",

}



type PsqlOrmDriver struct{

	OrmDriverBase

}


func init(){

	RegisterOrmDriver("postgres", &PsqlOrmDriver{})
}


func (this *PsqlOrmDriver) close(){
	this.db.Close()
}

func (this *PsqlOrmDriver) open(dbUrl string) error{

	common.InfoLog(dbUrl)

	db, err := sql.Open("postgres", dbUrl)
	if err != nil{
		return common.ErrorLog("open this failed", err)
	}

	if err = db.Ping(); err != nil{
		return common.ErrorLog(err)
	}

	this.db = db

	return nil
}

func (this *PsqlOrmDriver) Exists(modelInfo *DBModelInfo) (ok bool, err error){

	hsql := fmt.Sprintf("SELECT EXISTS( SELECT 1 FROM information_schema.tables WHERE table_name='%s')", modelInfo.TableName)
	common.InfoLog("insert op:", hsql)

	var res bool

	if err := this.db.QueryRow(hsql).Scan(&res); err != nil{
		return false, common.ErrorLog("check table exist failed:", modelInfo.TableName, err)
	}

	return res, nil
}


func (this *PsqlOrmDriver) Create(modelInfo *DBModelInfo) error{

	if ok, err := this.Exists(modelInfo); err != nil || ok == true{
		return common.ErrorLog("check exist failed")
	}

	var createColumnsBuffer bytes.Buffer

	for _, finfo := range modelInfo.fields{

		createColumnsBuffer.WriteString(finfo.name)

		if finfo.isAuto{
			createColumnsBuffer.WriteString(" serial primary key,")
			continue
		}

		switch finfo.Ftype {
		case TYPE_INT32:
			createColumnsBuffer.WriteString(" int")
		case TYPE_INT64:
			createColumnsBuffer.WriteString(" bigint")
		case TYPE_TIME:
			createColumnsBuffer.WriteString(" timestamp")
		case TYPE_STRING:
			createColumnsBuffer.WriteString(fmt.Sprintf(" varchar(%d)", finfo.size))
		case TYPE_REL:
			createColumnsBuffer.WriteString(" int")
		}

		if finfo.isPk{
			createColumnsBuffer.WriteString(" primary key")
		}

		createColumnsBuffer.WriteString(",")
	}

	createColsClause := createColumnsBuffer.String()
	createColsClause = createColsClause[0: len(createColsClause) -1]


	hsql := fmt.Sprintf("CREATE TABLE %s(%s)", modelInfo.TableName, createColsClause)
	common.InfoLog("insert op:", hsql)

	if _, err := this.db.Exec(hsql); err != nil{
		return common.ErrorLog("create table failed", err)
	}

	return nil
}




func (this *PsqlOrmDriver) fromTimeStr(tmStr string) (tm time.Time, err error){
	tm, err = time.Parse("2006-01-02 15:04:05", tmStr)
	if err != nil{
		return common.GetTimeNow(), common.ErrorLog("parse time failed:", tmStr)
	}
	return
}

func (this *PsqlOrmDriver) Insert(modelInfo *DBModelInfo, colNames, colVals []string) (idNum int32, err error){

	colNameStr := strings.Join(colNames, ",")
	//colValsStr := strings.Join(colVals, ",")

	colMarks := make([]string, len(colNames))
	insertCols := make([]interface{}, len(colNames))
	jj := 1
	for ii := range colMarks{
		colMarks[ii] = fmt.Sprintf("$%d", jj)
		insertCols[ii] = colVals[ii]
		jj += 1
	}

	colValsStr := strings.Join(colMarks, ",")

	var returnClause string
	if modelInfo.pkField != nil{
		returnClause = fmt.Sprintf("returning %s", modelInfo.pkField.name)
	}

	hsql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s) %s", modelInfo.TableName, colNameStr, colValsStr, returnClause)
	common.InfoLog("insert op:", hsql, insertCols)


	if err := this.db.QueryRow(hsql, insertCols...).Scan(&idNum); err != nil{
		return 0, common.ErrorLog("insert into table:", modelInfo.TableName, " failed, err:", err)
	}


	//todo when missing idnUm

	return
}


func (this *PsqlOrmDriver) Update(modelInfo *DBModelInfo, colNames, colVals []string) (int, error){

	var setClauseBuffer bytes.Buffer
	var whereClause string

	if len(colNames) < 2{
		return 0, common.ErrorLog("update sql col num is less than 2:", colNames, " colvals:", colVals)
	}

	upCols := make([]interface{}, 0)

	jj := 1
	for ii := 0; ii < len(colNames); ii++{

		colName := colNames[ii]
		colVal := colVals[ii]

		if colName == modelInfo.pkField.name{
			whereClause = fmt.Sprintf("where %s=%s", colName, colVal)
		}else{
			setClauseBuffer.WriteString(fmt.Sprintf("%s=$%d,", colName, jj))
			jj += 1
			upCols = append(upCols, colVal)
		}
	}

	if len(whereClause) <= 0{
		return 0, common.ErrorLog("update where cannot be null")
	}

	setClause := setClauseBuffer.String()
	setClause = setClause[0:len(setClause)-1]

	hsql := fmt.Sprintf("UPDATE %s SET %s %s", modelInfo.TableName, setClause, whereClause)
	common.InfoLog("udpate op:", hsql)

	res, err := this.db.Exec(hsql, upCols...)
	if err != nil{
		return 0, common.ErrorLog("update table:", modelInfo.TableName, " failed, err:", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil{
		return 0, common.ErrorLog("update table:", modelInfo.TableName, " rows affected failed, err:", err)
	}
	//todo when missing idnUm

	return int(rowsAffected), nil
}

func (this *PsqlOrmDriver) Delete(modelInfo *DBModelInfo, colNames, colVals []string) (int, error){

	var whereClauseBuffer bytes.Buffer
	whereClauseBuffer.WriteString("where ")
	for ii:= 0 ;ii < len(colNames); ii++{
		whereClauseBuffer.WriteString(fmt.Sprintf("%s=%s", colNames[ii], colVals[ii]))

		if ii < len(colNames) -1{
			whereClauseBuffer.WriteString(" and ")
		}
	}

	hsql := fmt.Sprintf("DELETE FROM %s %s", modelInfo.TableName, whereClauseBuffer.String())
	common.InfoLog("delete op:", hsql)

	res, err := this.db.Exec(hsql)
	if err != nil{
		return 0, common.ErrorLog("delete table:", modelInfo.TableName, " failed, err:", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil{
		return 0, common.ErrorLog("delete table:", modelInfo.TableName, " rows affected failed, err:", err)
	}
	//todo when missing idnUm

	return int(rowsAffected), nil
}


func (this *PsqlOrmDriver) Count(modelInfo *DBModelInfo) (count int, err error){

	hsql := fmt.Sprintf("SELECT n_live_tup FROM pg_stat_user_tables WHERE relname='%s'", modelInfo.TableName)
	common.InfoLog("insert op:", hsql)

	if err := this.db.QueryRow(hsql).Scan(&count); err != nil{
		return 0, common.ErrorLog("insert into table:", modelInfo.TableName, " failed, err:", err)
	}

	return
}

func (this *PsqlOrmDriver) Select(ormExpr *XOrmEpr, colNames, colVals, retNames, filters []string, pageClause string, orderClause string) (resArr []interface{}, err error){

	var retColsClause string
	var retColsClauseBuffer bytes.Buffer

	if ormExpr.Data.IsReturnFullMoel {
		for _, fname  := range ormExpr.ModelInfo.fieldNames{
			retColsClauseBuffer.WriteString(fmt.Sprintf("%s,", fname))
		}
	}else{
		for _, fname  := range retNames{
			retColsClauseBuffer.WriteString(fmt.Sprintf("%s,", fname))
		}
	}

	_retColsClause := retColsClauseBuffer.String()
	retColsClause = _retColsClause[:len(_retColsClause)-1]


	var whereClauseBuffer bytes.Buffer
	if len(colNames) > 0{
		whereClauseBuffer.WriteString("where ")
		for ii:= 0 ;ii < len(colNames); ii++{
			whereClauseBuffer.WriteString(fmt.Sprintf("%s=%s", colNames[ii], colVals[ii]))
			if ii < len(colNames) -1{
				whereClauseBuffer.WriteString(" and ")
			}
		}
	}


	filterClause := strings.Join(filters, " and ")

	var whereClause string
	if len(filterClause) > 0 && len(whereClause) > 0{
		whereClause = fmt.Sprintf("%s and %s",whereClauseBuffer.String(), filterClause)
	}else if len(filterClause) > 0 && len(whereClause) <= 0 {
		whereClause = fmt.Sprintf("where %s", filterClause)
	}else{
		whereClause = whereClauseBuffer.String()
	}

	hsql := fmt.Sprintf("SELECT %s FROM %s %s %s %s", retColsClause, ormExpr.ModelInfo.TableName, whereClause, orderClause, pageClause)
	common.InfoLog("select op:", hsql)
	rows, err := this.db.Query(hsql)
	if err != nil{
		return nil, common.ErrorLog("select from table:", ormExpr.ModelInfo.TableName, " failed, err:", err)
	}

	var scanArgs []interface{}

	if ormExpr.Data.IsReturnFullMoel {
		scanArgs = make([]interface{}, len(ormExpr.ModelInfo.fieldNames))
	}else{
		scanArgs = make([]interface{}, len(ormExpr.Data.RetNames))
	}


	for i := range scanArgs{
		var arg interface{}
		scanArgs[i] = &arg
	}

	resArr = make([]interface{}, 0, 1)


	for rows.Next(){

		if err = rows.Scan(scanArgs...); err != nil{
			return nil, common.ErrorLog("scan failed table:", ormExpr.ModelInfo.TableName, err)
		}

		if ormExpr.Data.IsReturnFullMoel {
			res, err := ormExpr.createNewFromScanArgs(scanArgs)
			if err != nil{
				return nil, common.ErrorLog("create new from scan args failed", scanArgs)
			}

			resArr = append(resArr, res)
		}else{
			resDic := make(map[string]string)

			for jj, arg := range scanArgs{

				//fname := retNames[jj]
				//finfo := ormExpr.ModelInfo.FieldDic[fname]

				str, err := common.EncodeStringForm(arg)
				if err != nil{
					return nil, common.ErrorLog("encode form failed", ormExpr.ModelInfo.TableName, retNames[jj], arg)
				}
				resDic[retNames[jj]] = str
			}
			resArr = append(resArr, resDic)
		}

	}
	//todo when missing idnUm

	return
}




func (this *PsqlOrmDriver) AddIndex(modelInfo *DBModelInfo, colName string) error {


	hsql := fmt.Sprintf("CREATE INDEX %s_idx ON %s(%s)",colName , modelInfo.TableName, colName)

	res, err := this.db.Exec(hsql)
	if err != nil{
		return common.ErrorLog("select from table:", modelInfo.TableName, " failed, err:", err)
	}
	common.InfoLog("add index :", res)

	return nil
}











