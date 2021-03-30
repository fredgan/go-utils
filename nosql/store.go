package nosql

import (
	"errors"
)

var ErrNil = errors.New("nil return")
var ErrWrongType = errors.New("wrong type")
var ErrWrongArgsNum = errors.New("args num error")

var ErrConnectionFailed = errors.New("store connect fail.")
var ErrErrPoolExhausted = errors.New("connection poll exhausted.")
var ErrExecuteFailed = errors.New("store Execute Command fail.")

type Reply interface {
	Int(interface{}, error) (int, error)
	Int64(interface{}, error) (int64, error)
	Uint64(interface{}, error) (uint64, error)
	Float64(interface{}, error) (float64, error)
	Bool(interface{}, error) (bool, error)
	Bytes(interface{}, error) ([]byte, error)
	String(interface{}, error) (string, error)
	Strings(interface{}, error) ([]string, error)
	Values(interface{}, error) ([]interface{}, error)
}

type kvStore interface {
	Get(string) (interface{}, error)
	Set(string, interface{}) error
	GetSet(string, interface{}) (interface{}, error)
	SetEx(string, interface{}, int64) error
	SetNx(string, interface{}) (int64, error)
	// nil表示成功，ErrNil表示数据库内已经存在这个key，其他表示数据库发生错误
	SetNxEx(string, interface{}, int64) error
	MGet(keys ...string) ([]interface{}, error)
	MSet(kvs map[string]interface{}) error
	MSetNX(kvs map[string]interface{}) (bool, error)
	SetBit(key string, offset uint32, value int) (int, error)
	Del(...string) (int64, error)
	Incr(string) (int64, error)
	IncrBy(string, int64) (int64, error)
	Expire(string, int64) (int64, error)
	ExpireAt(string, int64) (int64, error)
	TTl(string) (int64, error)
	Exists(string) (bool, error)
}

type KVStore interface {
	Reply
	kvStore
}

type hashStore interface {
	HGet(string, string) (interface{}, error)
	HSet(string, string, interface{}) error
	HMGet(string, ...string) (interface{}, error)
	HMSet(string, ...interface{}) error
	HExpire(string, int64) error
	HKeys(string) ([]string, error)
	HVals(string) ([]interface{}, error)
	HGetAll(string) (map[string]interface{}, error)
	HIncrBy(string, string, int64) (int64, error)
	HDel(string, ...string) (int64, error)
	HClear(string) error
	HLen(string) (int64, error)
}

type HashStore interface {
	Reply
	hashStore
}

type setStore interface {
	SAdd(string, ...interface{}) (int64, error)
	SPop(string) ([]byte, error)
	SIsMember(string, interface{}) (bool, error)
	SRem(string, ...interface{}) (int64, error)
	SMembers(string) ([]string, error)
}

type SetStore interface {
	Reply
	setStore
}

type zSetStore interface {
	ZAdd(string, ...interface{}) (int64, error)
	ZRem(string, ...string) (int64, error)
	ZExpire(string, int64) error
	ZRange(string, int64, int64, bool) (interface{}, error)
	ZRangeByScoreWithScore(string, int64, int64) (map[string]int64, error)
	ZClear(string) error
}

type ZSetStore interface {
	Reply
	zSetStore
}

type listStore interface {
	LRange(string, int64, int64) (interface{}, error)
	LLen(string) (int64, error)
	LRem(string, int, interface{}) (int, error)
	LPop(string) (interface{}, error)
	RPop(string) (interface{}, error)
	BLPop(string, int) (interface{}, error)
	BRPop(string, int) (interface{}, error)
	LPush(string, ...interface{}) error
	RPush(string, ...interface{}) error
	BRPopLPush(string, string, int) (interface{}, error)
	RPopLPush(string, string) (interface{}, error)
	LSet(string, int, interface{}) error
}

type evalStore interface {
	ScriptLoad(string) (interface{}, error)
	EvalSha(string, int, ...interface{}) (interface{}, error)
	Eval(string, int, ...interface{}) (interface{}, error)
}

type ListStore interface {
	Reply
	listStore
}

type HashZSetStore interface {
	Reply
	hashStore
	zSetStore
}

type KVHashStore interface {
	Reply
	kvStore
	hashStore
}

type Store interface {
	Reply
	kvStore
	hashStore
	zSetStore
	listStore
	setStore
	evalStore
	SetMaxIdle(int)
	SetMaxActive(int)
}
