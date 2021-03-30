package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"github.com/toontong/sqlz"

	"github.com/fredgan/go-utils/log"
	"github.com/fredgan/go-utils/sync"
)

var ErrNoRows = sql.ErrNoRows

var ErrReadOnly = Error{&mysql.MySQLError{ErrReadOnlyMode, "ReadOnly mode was open."}}

var ErrConnectFail = errors.New("connect fail.")

var ErrMasterSlaveSimilar = errors.New("master and salve is similar.")

const (
	DefaultSlowLogTimeout    = 500 * time.Millisecond //毫秒
	DefaultMaxOpenConns      = 800
	DefaultConnectWaiTimeout = 15 * time.Second
	DefaultCharset           = "utf8"
)

type Error struct {
	*mysql.MySQLError
}

type Result struct {
	LastInsertId int64
	RowsAffected int64
}

type WrapDB struct {
	*sqlx.DB
	Status *Status
	logger *SqlLogger

	slowLogTimeout time.Duration
	sp             sync.Semaphore
	OpenSqlLog     sync.AtomicBool
	readOnlyMode   bool

	errConnectPoolWaitTimeout Error
	connectWaitTimeout        time.Duration

	host   string
	dbname string
}

func openWrapDB(host string, port int, user string, password string, dbname string, maxIdleConns int) (*WrapDB, error) {
	maxConnection := DefaultMaxOpenConns
	connectWaitTimeout := DefaultConnectWaiTimeout
	return openWrapDBEx(host, port, user, password, dbname, maxIdleConns, maxConnection, connectWaitTimeout, DefaultCharset)
}

// OpenEx 提供了其他参数。关于 DSN，详见： https://github.com/go-sql-driver/mysql#dsn-data-source-name
// timeout 的格式与 time.ParseDuration 一致。
// charset 以 mysql 提供的为准，详见：https://dev.mysql.com/doc/refman/5.5/en/charset-charsets.html
func openWrapDBEx(host string, port int, user string, password string, dbname string,
	maxIdleConns int, maxConnection int, connectWaitTimeout time.Duration, charset string) (*WrapDB, error) {

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=%s&charset=%s",
		user, password, host, port, dbname, connectWaitTimeout.String(), charset)
	sqlxDB, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// SetMaxIdleConns已实现keepalive功能
	// 并不会出mysql gone way 错
	sqlxDB.SetMaxIdleConns(maxIdleConns)

	// sqlx.SetMaxOpenConns 有一个不足：
	// 当连接已满时，再请求一个新的query，此时处于无限等待 其它goroutine 释放 连接，
	// 上层调用代码不能设 锁timeout，也不能知道 是sql执行慢，还是等 获取可用连接慢
	sqlxDB.SetMaxOpenConns(maxConnection)
	// 所以此处增加一个连接申请的timeout功能sync.NewSemaphore
	sp := sync.NewSemaphore(maxConnection)

	status := &Status{}
	db := &WrapDB{DB: sqlxDB, Status: status, logger: nil, slowLogTimeout: DefaultSlowLogTimeout, sp: sp, readOnlyMode: false, host: host, dbname: dbname}

	db.errConnectPoolWaitTimeout = Error{&mysql.MySQLError{ErrTooManyUserConnections,
		fmt.Sprintf("Max Connections[%d] was Setting by app init. apply a new connect just wait[%s], but timeout",
			maxConnection, connectWaitTimeout)}}
	db.connectWaitTimeout = connectWaitTimeout

	// 没必需keepalive，忙时连接经常使用，肯定会自动保持
	// 闲时，重建一个连接的耗时几乎可以不计。
	// go db.keepalive()
	return db, nil
}

func (db *WrapDB) SetReadOnly(readOnly bool) {
	db.readOnlyMode = readOnly
}

func (db *WrapDB) SetSlowLogTimeout(t time.Duration) {
	if t < time.Millisecond {
		t = t * time.Microsecond
	}
	db.slowLogTimeout = t
}

func (db *WrapDB) SetLogger(l *SqlLogger) {
	db.logger = l
}

func (db *WrapDB) Query(dest interface{}, query string, args ...interface{}) error {
	//TODO add slow log, large rows log
	if db.readOnlyMode {
		isSelect := strings.HasPrefix(query, "SELECT") || strings.HasPrefix(query, "select")
		isSelect = isSelect && !strings.Contains(query, "FOR UPDATE")
		isReadQuery := isSelect || strings.HasPrefix(query, "SHOW") || strings.HasPrefix(query, "show")

		if !isReadQuery {
			return ErrReadOnly
		}
	}

	start := time.Now()
	if !db.sp.AcquireTimeout(db.connectWaitTimeout) {
		log.Fatal("connect to MySQL[%s/%s] %s", db.host, db.dbname, db.errConnectPoolWaitTimeout.Message)
		return db.errConnectPoolWaitTimeout
	}

	if time.Since(start) > db.slowLogTimeout {
		log.Warn("wait for acquire a mysql connection use time[%s] too long,querying=%d|executing=%d",
			time.Since(start), db.Status.Querying.Get(), db.Status.Executing.Get())
	}
	start = time.Now()

	db.Status.Querying.Add(1)

	var rowsReturned int
	defer func() {
		db.sp.Release()
		db.Status.Querying.Add(-1)
		duration := time.Since(start)
		db.Status.UsedTime.Add(duration)
		if db.OpenSqlLog.Get() && db.logger != nil {
			db.logger.Write(duration, query)
			sqlz.Z(query)
		}
		if duration > db.slowLogTimeout {
			log.Error("mysql.Query took [%s] sec,sql=[%s]|rowsReturned=%d|querying=%d|executing=%d",
				duration, query, rowsReturned, db.Status.Querying.Get(), db.Status.Executing.Get())

			action, tb := sqlz.ParseQuery(query)
			log.Error("mysql.Query took [%s] sec,sql=[%s %s]|rowsReturned=%d|querying=%d|executing=%d",
				duration, action, tb, rowsReturned, db.Status.Querying.Get(), db.Status.Executing.Get())
		}
	}()

	db.Status.QueryCount.Add(1)
	t := reflect.TypeOf(dest)
	if reflectx.Deref(t).Kind() == reflect.Slice {
		err := db.DB.Select(dest, query, args...)
		if err != nil && err != ErrNoRows {
			db.Status.ErrCount.Add(1)
			log.Error("mysql.DB.Select() err=[%s] sql=[%s]", err.Error(), query)
		} else if err == nil {
			rowsReturned = reflect.ValueOf(dest).Elem().Len()
		}
		return err
	}

	err := db.DB.Get(dest, query, args...)
	if err != nil && err != ErrNoRows {
		db.Status.ErrCount.Add(1)
		log.Error("mysql.DB.Get() err=[%s], sql=[%s]", err.Error(), query)
	} else if err == nil {
		rowsReturned = 1
	}
	return err
}

func (db *WrapDB) QueryInt64(query string) (int64, error) {
	//如果结果为NULL， 则返回0
	var num NullInt64
	err := db.Query(&num, query)
	return num.Int64, err
}

func (db *WrapDB) QueryString(query string) (string, error) {
	var str NullString
	err := db.Query(&str, query)

	return str.String, err
}

func ToMySQLError(e error) *Error {
	if me, ok := e.(*mysql.MySQLError); ok {
		return &Error{me}
	}
	return nil
}

func ErrorEqual(e error, code uint16) bool {
	me := ToMySQLError(e)
	return me != nil && me.Number == code
}

func (db *WrapDB) Exec(query string, args ...interface{}) (*Result, error) {
	//TODO add  slow log, large rows log
	if db.readOnlyMode {
		return nil, ErrReadOnly
	}
	start := time.Now()
	if !db.sp.AcquireTimeout(db.connectWaitTimeout) {
		log.Fatal("connect to MySQL[%s/%s] %s", db.host, db.dbname, db.errConnectPoolWaitTimeout.Message)
		return nil, db.errConnectPoolWaitTimeout
	}

	if time.Since(start) > db.slowLogTimeout {
		log.Warn("wait for acquire a mysql connection use time[%s] too long", time.Since(start))
	}
	start = time.Now()

	db.Status.Executing.Add(1)

	var rowsAffected int64
	defer func() {
		db.sp.Release()
		db.Status.Executing.Add(-1)
		duration := time.Since(start)
		db.Status.UsedTime.Add(duration)
		if db.OpenSqlLog.Get() && db.logger != nil {
			db.logger.Write(duration, query)
			sqlz.Z(query)
		}
		if duration > db.slowLogTimeout {
			log.Error("mysql.Exec took [%s] sec; sql=[%s]|rowsAffected=%d|executing=%d|querying=%d",
				duration, query, rowsAffected, db.Status.Executing.Get(), db.Status.Querying.Get())

			action, tb := sqlz.ParseQuery(query)
			log.Error("mysql.Exec took [%s] sec; sql=[%s %s]|rowsAffected=%d|executing=%d|querying=%d",
				duration, action, tb, rowsAffected, db.Status.Executing.Get(), db.Status.Querying.Get())
		}
	}()
	db.Status.ExecCount.Add(1)
	r, err := db.DB.Exec(query, args...)

	if err != nil {
		if !ErrorEqual(err, ErrDupEntry) {
			db.Status.ErrCount.Add(1)
			log.Error("mysql.DB.Exec err=[%s] query=[%s]", err.Error(), query)
		}
		return nil, err
	}
	lastInsertId, err := r.LastInsertId()
	if err != nil {
		db.Status.ErrCount.Add(1)
		log.Error("mysql.Result.LastInsertId err=[%s]", err.Error())
		return nil, err
	}
	rowsAffected, err = r.RowsAffected()

	if err != nil {
		db.Status.ErrCount.Add(1)
		log.Error("mysql.Result.RowsAffected err=[%s]", err.Error())
		return nil, err
	}
	return &Result{lastInsertId, rowsAffected}, nil
}

func (db *WrapDB) Close() {
	if err := db.DB.Close(); err != nil {
		log.Error("mysql.DB.Close err=[%s]", err.Error())
	}
}
