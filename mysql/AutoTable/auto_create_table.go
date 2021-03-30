package AutoTable

import (
	"github.com/fredgan/go-utils/mysql"
	"github.com/fredgan/go-utils/log"
)

type TableCreater interface {
	GetCreateTableSql(arg interface{}) string
}

type AutoCreateTable struct {
	creater TableCreater
}

func NewAutoCreateTable(creater TableCreater) *AutoCreateTable {
	return &AutoCreateTable{creater}
}

func (self *AutoCreateTable) Exec(db *mysql.DB, sql string, createTableArg interface{}) (*mysql.Result, error) {
	if res, err := db.Exec(sql); err == nil {
		log.Debug("sql exce,sql=%s", sql)
		return res, nil

	} else {
		if !mysql.ErrorEqual(err, mysql.ErrNoSuchTable) {
			log.Error("sql exec error,sql=%s,error=%s", sql, err.Error())
			return nil, err
		}

		if err := self.createTable(db, createTableArg); err != nil {
			return nil, err
		}

		res, err := db.Exec(sql)
		if err != nil {
			log.Error("sql exec error,sql=%s,error=%s", sql, err.Error())
			return nil, err
		}

		log.Debug("sql exce,sql=%s", sql)
		return res, nil
	}
}

func (self *AutoCreateTable) Query(db *mysql.DB, dest interface{}, sql string, createTableArg interface{}) error {

	if err := db.Query(dest, sql); err == nil {
		log.Debug("sql query,sql=%s", sql)
		return nil

	} else {
		if !mysql.ErrorEqual(err, mysql.ErrNoSuchTable) {
			if err == mysql.ErrNoRows {
				log.Info("sql query error,sql=%s,error=%s", sql, err.Error())
			} else {
				log.Error("sql query error,sql=%s,error=%s", sql, err.Error())
			}
			return err
		}

		if err := self.createTable(db, createTableArg); err != nil {
			return err
		}

		if err := db.Query(dest, sql); err != nil {
			if err == mysql.ErrNoRows {
				log.Info("sql query error,sql=%s,error=%s", sql, err.Error())
			} else {
				log.Error("sql query error,sql=%s,error=%s", sql, err.Error())
			}
			return err
		}

		log.Debug("sql query,sql=%s", sql)
		return nil
	}
}

func (self *AutoCreateTable) QueryInt64(db *mysql.DB, sql string, createTableArg interface{}) (int64, error) {

	if rs, err := db.QueryInt64(sql); err == nil {
		log.Debug("sql query,sql=%s, result=%d", sql, rs)
		return rs, nil

	} else {
		if !mysql.ErrorEqual(err, mysql.ErrNoSuchTable) {
			log.Error("sql query error,sql=%s,error=%s", sql, err.Error())
			return 0, err
		}

		if err := self.createTable(db, createTableArg); err != nil {
			return 0, err
		}

		rs, err := db.QueryInt64(sql)
		if err != nil {
			log.Error("sql query error,sql=%s,error=%s", sql, err.Error())
			return 0, err
		}

		log.Debug("sql query,sql=%s, result=%d", sql, rs)
		return rs, nil
	}
}

func (self *AutoCreateTable) createTable(db *mysql.DB, arg interface{}) error {

	sql := self.creater.GetCreateTableSql(arg)

	if _, err := db.Exec(sql); err != nil {
		log.Error("add createTable error, error=%s, sql=%s", err.Error(), sql)
		return err
	}

	log.Info("create table %s", sql)

	return nil
}
