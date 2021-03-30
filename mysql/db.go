package mysql

import (
	"time"
)

type DBConfig struct {
	Host               string
	Port               int
	User               string
	Password           string
	DBName             string
	MaxIdleConns       int
	MaxConnection      int
	ConnectWaitTimeout time.Duration
	Charset            string
}

func Open(host string, port int, user string, password string, dbname string, maxIdleConns int) (*DB, error) {
	db, err := openWrapDB(host, port, user, password, dbname, maxIdleConns)
	if err != nil {
		return nil, err
	}

	return newDb(db), nil
}

// OpenEx 提供了其他参数。关于 DSN，详见： https://github.com/go-sql-driver/mysql#dsn-data-source-name
// timeout 的格式与 time.ParseDuration 一致。
// charset 以 mysql 提供的为准，详见：https://dev.mysql.com/doc/refman/5.5/en/charset-charsets.html
func OpenEx(host string, port int, user string, password string, dbname string,
	maxIdleConns int, maxConnection int, connectWaitTimeout time.Duration, charset string) (*DB, error) {
	db, err := openWrapDBEx(host, port, user, password, dbname, maxIdleConns, maxConnection, connectWaitTimeout, charset)
	if err != nil {
		return nil, err
	}
	return newDb(db), nil
}

func OpenConn(cfg *DBConfig) (*DB, error) {
	setDefaultConfig(cfg)

	return OpenEx(cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName,
		cfg.MaxIdleConns, cfg.MaxConnection, cfg.ConnectWaitTimeout, cfg.Charset)
}

func OpenMasterSlave(masterCfg *DBConfig, slaveCfg *DBConfig) (*DB, error) {
	isSameEntity := masterCfg.Host == slaveCfg.Host && masterCfg.Port == slaveCfg.Port
	if isSameEntity {
		return nil, ErrMasterSlaveSimilar
	}

	setDefaultConfig(masterCfg)

	masterDB, err := openWrapDBEx(masterCfg.Host, masterCfg.Port, masterCfg.User, masterCfg.Password, masterCfg.DBName,
		masterCfg.MaxIdleConns, masterCfg.MaxConnection, masterCfg.ConnectWaitTimeout, masterCfg.Charset)

	if err != nil {
		return nil, err
	}

	setDefaultConfig(slaveCfg)
	slaveDB, err := openWrapDBEx(slaveCfg.Host, slaveCfg.Port, slaveCfg.User, slaveCfg.Password, slaveCfg.DBName,
		slaveCfg.MaxIdleConns, slaveCfg.MaxConnection, slaveCfg.ConnectWaitTimeout, slaveCfg.Charset)

	if err != nil {
		return nil, err
	}

	return newDbEx(masterDB, slaveDB), nil
}

func setDefaultConfig(cfg *DBConfig) {
	if cfg.MaxConnection == 0 {
		cfg.MaxConnection = DefaultMaxOpenConns
	}

	if cfg.ConnectWaitTimeout == 0 {
		cfg.ConnectWaitTimeout = DefaultConnectWaiTimeout
	}

	if len(cfg.Charset) == 0 {
		cfg.Charset = DefaultCharset
	}
}

func newDb(masterDB *WrapDB) *DB {
	return &DB{
		WrapDB:  masterDB,
		slaveDB: masterDB,
	}
}

func newDbEx(masterDB *WrapDB, slaveDB *WrapDB) *DB {
	if masterDB != slaveDB {
		slaveDB.SetReadOnly(true)
	}

	return &DB{
		WrapDB:  masterDB,
		slaveDB: slaveDB,
	}
}

type DB struct {
	*WrapDB
	slaveDB *WrapDB
	logger  *SqlLogger
}

func (db *DB) SetReadOnly(readOnly bool) {
	db.WrapDB.SetReadOnly(readOnly)
}

func (db *DB) SetSlowLogTimeout(t time.Duration) {
	db.WrapDB.SetSlowLogTimeout(t)
	if db.WrapDB != db.slaveDB {
		db.slaveDB.SetSlowLogTimeout(t)
	}
}

func (db *DB) SetLogger(l *SqlLogger) {
	if db.logger != nil {
		db.logger.Close()
	}
	db.logger = l

	db.WrapDB.SetLogger(db.logger)
	if db.WrapDB != db.slaveDB {
		db.slaveDB.SetLogger(db.logger)
	}
}

func (db *DB) Query(dest interface{}, query string, args ...interface{}) error {
	return db.slaveDB.Query(dest, query, args...)
}

func (db *DB) QueryInt64(query string) (int64, error) {
	return db.slaveDB.QueryInt64(query)
}

func (db *DB) QueryString(query string) (string, error) {
	return db.slaveDB.QueryString(query)
}

func (db *DB) Exec(query string, args ...interface{}) (*Result, error) {
	return db.WrapDB.Exec(query, args...)
}

func (db *DB) Close() {
	db.WrapDB.Close()
	if db.WrapDB != db.slaveDB {
		db.slaveDB.Close()
	}

	if db.logger != nil {
		db.logger.Close()
	}
}
