package nosql

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/fredgan/go-utils/util"
	"github.com/fredgan/go-utils/log"
	sync2 "github.com/fredgan/go-utils/sync"
)

const redisMaxIdleConn = 64
const redisMaxActive = 512

type RedisStore struct {
	pool        *redis.Pool
	host        string
	port        int
	db          int
	stat        *redisStat
	withTimeout bool

	semaphore sync2.Semaphore // for the connection-pool
}

func newRedisStore(host string, port int, db int, withTimeout bool) (*RedisStore, error) {
	f := func() (redis.Conn, error) {
		var c redis.Conn
		var err error
		if withTimeout {
			var connectTimeout, readTimeout, writeTimeout time.Duration = time.Second * 10, time.Second * 3, time.Second * 3
			c, err = redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port),
				connectTimeout, readTimeout, writeTimeout)
		} else {
			c, err = redis.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
		}
		if err != nil {
			log.Error("redis(%s:%d) connect err=[%v]", host, port, err)
			return nil, ErrConnectionFailed
		}
		if _, err := c.Do("SELECT", db); err != nil {
			log.Error("[redis] select db err=%s", err.Error())
		}
		return c, err
	}
	pool := redis.NewPool(f, redisMaxIdleConn)
	pool.MaxActive = redisMaxActive
	pool.Wait = true

	store := &RedisStore{
		pool: pool,
		host: host, port: port, db: db,
		withTimeout: withTimeout,
	}
	store.semaphore = sync2.NewSemaphore(redisMaxActive)
	store.stat = newRedisStat(store)
	return store, nil
}

func NewRedisStore(host string, port int, db int) (*RedisStore, error) {
	return newRedisStore(host, port, db, true)
}

func NewRedisStoreWithoutTimeout(host string, port int, db int) (*RedisStore, error) {
	return newRedisStore(host, port, db, false)
}

func (r *RedisStore) SetMaxIdle(maxIdle int) {
	r.pool.MaxIdle = maxIdle
}

func (r *RedisStore) SetMaxActive(maxActive int) {
	r.pool.MaxActive = maxActive
	r.semaphore = sync2.NewSemaphore(maxActive)
}

func (r *RedisStore) Get(key string) (interface{}, error) {
	return r.do("GET", key)
}

func (r *RedisStore) Set(key string, value interface{}) error {
	_, err := r.do("SET", key, value)
	return err
}

func (r *RedisStore) GetSet(key string, value interface{}) (interface{}, error) {
	return r.do("GETSET", key, value)
}

func (r *RedisStore) SetNx(key string, value interface{}) (int64, error) {
	return r.Int64(r.do("SETNX", key, value))
}

func (r *RedisStore) SetEx(key string, value interface{}, timeout int64) error {
	_, err := r.do("SETEX", key, timeout, value)
	return err
}

// nil表示成功，ErrNil表示数据库内已经存在这个key，其他表示数据库发生错误
func (r *RedisStore) SetNxEx(key string, value interface{}, timeout int64) error {
	_, err := r.String(r.do("SET", key, value, "NX", "EX", timeout))
	return err
}

func (r *RedisStore) MGet(keys ...string) ([]interface{}, error) {
	args := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}
	return r.Values(r.do("MGET", args...))
}

func (r *RedisStore) MSet(kvs map[string]interface{}) error {
	pairs := make([]interface{}, 0, len(kvs)*2)
	for k, v := range kvs {
		pairs = append(pairs, k, v)
	}

	_, err := r.do("MSET", pairs...)
	return err
}

func (r *RedisStore) MSetNX(kvs map[string]interface{}) (bool, error) {
	pairs := make([]interface{}, 0, len(kvs)*2)
	for k, v := range kvs {
		pairs = append(pairs, k, v)
	}

	return r.Bool(r.do("MSETNX", pairs...))
}

func (r *RedisStore) ExpireAt(key string, timestamp int64) (int64, error) {
	return r.Int64(r.do("EXPIREAT", key, timestamp))
}

func (r *RedisStore) Del(keys ...string) (int64, error) {
	ks := make([]interface{}, len(keys))
	for i, key := range keys {
		ks[i] = key
	}
	return r.Int64(r.do("DEL", ks...))
}

func (r *RedisStore) Incr(key string) (int64, error) {
	return r.Int64(r.do("INCR", key))
}

func (r *RedisStore) IncrBy(key string, delta int64) (int64, error) {
	return r.Int64(r.do("INCRBY", key, delta))
}

func (r *RedisStore) Expire(key string, duration int64) (int64, error) {
	return r.Int64(r.do("EXPIRE", key, duration))
}

func (r *RedisStore) Exists(key string) (bool, error) {
	return r.Bool(r.do("EXISTS", key))
}

func (r *RedisStore) HGet(key string, field string) (interface{}, error) {
	return r.do("HGET", key, field)
}

func (r *RedisStore) HLen(key string) (int64, error) {
	return r.Int64(r.do("HLEN", key))
}

func (r *RedisStore) HSet(key string, field string, val interface{}) error {
	_, err := r.do("HSET", key, field, val)
	return err
}

func (r *RedisStore) HDel(key string, fields ...string) (int64, error) {
	ks := make([]interface{}, len(fields)+1)
	ks[0] = key
	for i, key := range fields {
		ks[i+1] = key
	}
	return r.Int64(r.do("HDEL", ks...))
}

func (r *RedisStore) HClear(key string) error {
	_, err := r.do("DEL", key)
	return err
}

func (r *RedisStore) HMGet(key string, fields ...string) (interface{}, error) {
	if len(fields) == 0 {
		return nil, ErrNil
	}
	args := make([]interface{}, len(fields)+1)
	args[0] = key
	for i, field := range fields {
		args[i+1] = field
	}
	return r.do("HMGET", args...)

}

func (r *RedisStore) HMSet(key string, kvs ...interface{}) error {
	if len(kvs) == 0 {
		return nil
	}
	if len(kvs)%2 != 0 {
		return ErrWrongArgsNum
	}
	args := make([]interface{}, len(kvs)+1)
	args[0] = key
	for i := 0; i < len(kvs); i += 2 {
		if _, ok := kvs[i].(string); !ok {
			return errors.New("field must be string")
		}
		args[i+1] = kvs[i]
		args[i+2] = kvs[i+1]
	}
	_, err := r.do("HMSET", args...)
	return err
}

func (r *RedisStore) HExpire(key string, duration int64) error {
	_, err := r.do("EXPIRE", key, duration)
	return err
}

func (r *RedisStore) HKeys(key string) ([]string, error) {
	hkeys, err := r.Strings(r.do("HKEYS", key))
	if err != nil {
		return nil, err
	}

	return hkeys, nil
}

func (r *RedisStore) HVals(key string) ([]interface{}, error) {
	hvals, err := r.Values(r.do("HVALS", key))
	if err != nil {
		return nil, err
	}

	return hvals, nil
}

func (r *RedisStore) HGetAll(key string) (map[string]interface{}, error) {
	vals, err := r.Values(r.do("HGETALL", key))
	if err != nil {
		return nil, err
	}
	num := len(vals) / 2
	result := make(map[string]interface{}, num)
	for i := 0; i < num; i++ {
		key, _ := r.String(vals[2*i], nil)
		result[key] = vals[2*i+1]
	}
	return result, nil
}

func (r *RedisStore) HIncrBy(key, field string, delta int64) (int64, error) {
	return r.Int64(r.do("HINCRBY", key, field, delta))
}

func (r *RedisStore) ZAdd(key string, kvs ...interface{}) (int64, error) {
	if len(kvs) == 0 {
		return 0, nil
	}
	if len(kvs)%2 != 0 {
		return 0, errors.New("args num error")
	}
	args := make([]interface{}, len(kvs)+1)
	args[0] = key
	for i := 0; i < len(kvs); i += 2 {
		args[i+1] = kvs[i]
		args[i+2] = kvs[i+1]
	}
	return r.Int64(r.do("ZAdd", args...))
}

func (r *RedisStore) ZRem(key string, members ...string) (int64, error) {
	args := make([]interface{}, len(members)+1)
	args[0] = key
	for i, m := range members {
		args[i+1] = m
	}
	return r.Int64(r.do("ZREM", args...))
}

func (r *RedisStore) ZClear(key string) error {
	_, err := r.do("DEL", key)
	return err
}

func (r *RedisStore) ZExpire(key string, duration int64) error {
	_, err := r.do("EXPIRE", key, duration)
	return err
}

func (r *RedisStore) ZRange(key string, min, max int64, withScores bool) (interface{}, error) {
	if withScores {
		return r.do("ZRANGE", key, min, max, "WITHSCORES")
	} else {
		return r.do("ZRANGE", key, min, max)
	}
}

func (r *RedisStore) ZRangeByScoreWithScore(key string, min, max int64) (map[string]int64, error) {
	vals, err := r.Values(r.do("ZRANGEBYSCORE", key, min, max, "WITHSCORES"))
	if err != nil {
		return nil, err
	}
	n := len(vals) / 2
	result := make(map[string]int64, n)
	for i := 0; i < n; i++ {
		key, _ := r.String(vals[2*i], nil)
		score, _ := r.String(vals[2*i+1], nil)
		v, _ := strconv.ParseFloat(score, 64)
		result[key] = int64(v)
	}
	return result, nil
}

func (r *RedisStore) LRange(key string, start, stop int64) (interface{}, error) {
	return r.do("LRANGE", key, start, stop)
}

func (r *RedisStore) LLen(key string) (int64, error) {
	return r.Int64(r.do("LLEN", key))
}

func (r *RedisStore) LSet(key string, index int, value interface{}) error {
	_, err := r.do("LSET", key, index, value)
	return err
}

func (r *RedisStore) LRem(key string, count int, value interface{}) (int, error) {
	return r.Int(r.do("LREM", key, count, value))
}

func (r *RedisStore) TTl(key string) (int64, error) {
	return r.Int64(r.do("TTL", key))
}

func (r *RedisStore) LPop(key string) (interface{}, error) {
	return r.do("LPOP", key)
}

func (r *RedisStore) RPop(key string) (interface{}, error) {
	return r.do("RPOP", key)
}

func (r *RedisStore) BLPop(key string, timeout int) (interface{}, error) {
	vals, err := r.Values(r.do("BLPOP", key, timeout))
	if err != nil {
		return nil, err
	}
	return vals[1], nil
}

func (r *RedisStore) BRPop(key string, timeout int) (interface{}, error) {
	vals, err := r.Values(r.do("BRPOP", key, timeout))
	if err != nil {
		return nil, err
	}
	return vals[1], nil
}

func (r *RedisStore) LPush(key string, value ...interface{}) error {
	args := make([]interface{}, len(value)+1)
	args[0] = key
	for i, v := range value {
		args[i+1] = v
	}
	_, err := r.do("LPUSH", args...)
	return err
}

func (r *RedisStore) RPush(key string, value ...interface{}) error {
	args := make([]interface{}, len(value)+1)
	args[0] = key
	for i, v := range value {
		args[i+1] = v
	}
	_, err := r.do("RPUSH", args...)
	return err
}

func (r *RedisStore) BRPopLPush(srcKey string, destKey string, timeout int) (interface{}, error) {
	return r.do("BRPOPLPUSH", srcKey, destKey, timeout)
}

func (r *RedisStore) RPopLPush(srcKey string, destKey string) (interface{}, error) {
	return r.do("RPOPLPUSH", srcKey, destKey)
}

func (r *RedisStore) SAdd(key string, members ...interface{}) (int64, error) {
	args := make([]interface{}, len(members)+1)
	args[0] = key
	for i, m := range members {
		args[i+1] = m
	}
	return r.Int64(r.do("SADD", args...))
}

func (r *RedisStore) SPop(key string) ([]byte, error) {
	return r.Bytes(r.do("SPOP", key))
}

func (r *RedisStore) SIsMember(key string, member interface{}) (bool, error) {
	return r.Bool(r.do("SISMEMBER", key, member))
}

func (r *RedisStore) SRem(key string, members ...interface{}) (int64, error) {
	args := make([]interface{}, len(members)+1)
	args[0] = key
	for i, m := range members {
		args[1+i] = m
	}
	return r.Int64(r.do("SREM", args...))
}

func (r *RedisStore) SMembers(key string) ([]string, error) {
	values, err := r.Strings(r.do("SMEMBERS", key))
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (r *RedisStore) ScriptLoad(luaScript string) (interface{}, error) {
	return r.do("SCRIPT", "LOAD", luaScript)
}

func (r *RedisStore) EvalSha(sha1 string, numberKeys int, keysArgs ...interface{}) (interface{}, error) {
	return r.do("EVALSHA", append([]interface{}{sha1, numberKeys}, keysArgs...)...)
}

func (r *RedisStore) Eval(luaScript string, numberKeys int, keysArgs ...interface{}) (interface{}, error) {
	return r.do("EVAL", append([]interface{}{luaScript, numberKeys}, keysArgs...)...)
}

func (r *RedisStore) GetBit(key string, offset int64) (int64, error) {
	return r.Int64(r.do("GETBIT", key, offset))
}

const (
	default_timeout   = 3 * time.Second
	default_slow_time = 500 * time.Millisecond
)

func (r *RedisStore) do(cmd string, args ...interface{}) (interface{}, error) {
	start := time.Now()
	if !r.semaphore.AcquireTimeout(default_timeout) {
		log.Error("RedisStore acquire an connection[%s:%d] timeout=[%s], ActiveCount=[%d] MaxActive=[%d]",
			r.host, r.port, default_timeout, r.pool.ActiveCount(), r.pool.MaxActive)
		return nil, ErrErrPoolExhausted
	}

	if time.Since(start) > default_slow_time {
		log.Warn("Acquire a redis connection[%s:%d] use time[%s] too long, ActiveCount=[%d] MaxActive=[%d]",
			r.host, r.port, time.Since(start), r.pool.ActiveCount(), r.pool.MaxActive)
	}

	conn := r.pool.Get()
	r.stat.onStart()
	begin := time.Now()
	activteCount := r.pool.ActiveCount()
	res, err := conn.Do(cmd, args...)
	conn.Close()
	r.semaphore.Release()

	duration := time.Since(begin)
	r.stat.onDone(duration, err)

	if r.withTimeout && duration > default_slow_time {
		log.Error(`[redis] "RedisStore.do" "it took %.2f sec" "host=%s:%d|cmd=%s|args=%s|active_count=%d|max_idle=%d|max_active=%d" `,
			duration.Seconds(), r.host, r.port, cmd, util.Join(args, ","), activteCount,
			r.pool.MaxIdle, r.pool.MaxActive)
	}

	if err == redis.ErrNil {
		return nil, ErrNil
	}
	if err != nil {
		log.Error("[redis] execute cmd fail cmd=[%v] err=[%v]", cmd, err)
		return res, ErrExecuteFailed
	}
	return res, err
}

func (r *RedisStore) GetPool() *redis.Pool {
	return r.pool
}

func (r *RedisStore) SetBit(key string, offset uint32, value int) (int, error) {
	return r.Int(r.do("SETBIT", key, offset, value))
}
