package nosql

import (
	"math/rand"
	"strconv"
	"strings"
	"time"

	"gopkg.in/redis.v5"
	"github.com/fredgan/go-utils/log"
	"github.com/fredgan/go-utils/util"
)

const (
	default_connection_timeout = 10 * time.Second
	default_rw_timeout         = 3 * time.Second
	default_cluster_slow_time  = 500 * time.Millisecond

	default_pool_timeout                  = 10 * time.Second
	default_close_idle_connection_timeout = 10 * time.Second
	redisClusterMaxActive                 = 512

	slotNumber = 16384

	maxIdleSlotConns = 100
)

type Redis struct {
	client      redis.Cmdable
	addrs       []string
	withTimeout bool
}

func newRedisCluster(addrs []string, password string, withTimeout bool) (*Redis, error) {
	options := &redis.ClusterOptions{
		Addrs:       addrs,
		PoolSize:    redisClusterMaxActive,
		PoolTimeout: default_pool_timeout,
		IdleTimeout: default_close_idle_connection_timeout,
	}

	if withTimeout {
		options.DialTimeout = default_connection_timeout
		options.ReadTimeout = default_rw_timeout
		options.WriteTimeout = default_rw_timeout
	}

	client := redis.NewClusterClient(options)
	err := client.Ping().Err()
	if err != nil {
		return nil, err
	}

	redisCluster := new(Redis)
	redisCluster.client = client
	redisCluster.addrs = addrs

	return redisCluster, nil
}

func NewRedisCluster(addrs []string, password string) (*Redis, error) {
	return newRedisCluster(addrs, password, true)
}

func newRedisSingle(addr string, password string, withTimeout bool) (*Redis, error) {
	options := &redis.Options{
		DB:          0,
		Addr:        addr,
		PoolSize:    redisClusterMaxActive,
		PoolTimeout: default_pool_timeout,
		IdleTimeout: default_close_idle_connection_timeout,
	}

	if withTimeout {
		options.DialTimeout = default_connection_timeout
		options.ReadTimeout = default_rw_timeout
		options.WriteTimeout = default_rw_timeout
	}

	client := redis.NewClient(options)
	err := client.Ping().Err()
	if err != nil {
		return nil, err
	}

	redisSingle := new(Redis)
	redisSingle.client = client
	redisSingle.addrs = []string{addr}

	return redisSingle, nil
}

func NewRedisClusterWithoutTimeout(addrs []string, password string) (*Redis, error) {
	return newRedisCluster(addrs, password, false)
}

func NewRedisSingle(addr string, password string) (*Redis, error) {
	return newRedisSingle(addr, password, false)
}

func (r *Redis) SetMaxIdle(maxIdle int) {
}

func (r *Redis) SetMaxActive(maxActive int) {
}

func (r *Redis) key(k string) string {
	if s := strings.IndexByte(k, '{'); s > -1 {
		if e := strings.IndexByte(k[s+1:], '}'); e > 0 {
			return k[s+1 : s+e+1]
		}
	}
	return k
}

// hashSlot returns a consistent slot number between 0 and 16383
// for any given string key.
func (r *Redis) slot(key string) int64 {
	key = r.key(key)
	if key == "" {
		return int64(rand.Intn(slotNumber))
	}
	return int64(crc16sum(key)) % slotNumber
}

func (r *Redis) groupKeysBySlot(keys []string) map[int64][]string {
	m := make(map[int64][]string)

	for _, key := range keys {
		n := r.slot(key)
		if slotKeys, ok := m[n]; !ok || len(slotKeys) == 0 {
			m[n] = make([]string, 0)
		}

		m[n] = append(m[n], key)
	}

	return m
}

func (r *Redis) Get(key string) (interface{}, error) {
	defer r.elapse(r.start(), "GET", key)

	result, err := r.client.Get(key).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return []byte(result), nil
}

func (r *Redis) Set(key string, value interface{}) error {
	defer r.elapse(r.start(), "SET", key, value)

	err := r.client.Set(key, value, 0).Err()
	return r.convertError(err)
}

func (r *Redis) GetSet(key string, value interface{}) (interface{}, error) {
	defer r.elapse(r.start(), "GETSET", key, value)

	result, err := r.client.GetSet(key, value).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return []byte(result), nil
}

func (r *Redis) SetNx(key string, value interface{}) (int64, error) {
	defer r.elapse(r.start(), "SETNX", key, value)

	result, err := r.client.SetNX(key, value, 0).Result()
	if err != nil {
		return 0, r.convertError(err)
	}

	if result {
		return 1, nil
	}

	return 0, nil
}

func (r *Redis) SetEx(key string, value interface{}, timeout int64) error {
	defer r.elapse(r.start(), "SETEX", key, value, timeout)

	_, err := r.client.Set(key, value, time.Duration(timeout)*time.Second).Result()
	if err != nil {
		return r.convertError(err)
	}

	return nil
}

// nil表示成功，ErrNil表示数据库内已经存在这个key，其他表示数据库发生错误
func (r *Redis) SetNxEx(key string, value interface{}, timeout int64) error {
	defer r.elapse(r.start(), "SETNXEX", key, value, timeout)

	result, err := r.client.SetNX(key, value, time.Duration(timeout)*time.Second).Result()
	if err != nil {
		return r.convertError(err)
	}

	if result {
		return nil
	}

	return ErrNil
}

func (r *Redis) MGet(keys ...string) ([]interface{}, error) {
	defer r.elapse(r.start(), "MGET", keys)

	slots := r.groupKeysBySlot(keys)
	resChan := make(chan map[string]interface{}, len(slots))
	defer func() {
		close(resChan)
	}()

	fns := []Fn{}
	for _, slotKeys := range slots {
		sk := slotKeys
		fn := func() error {
			res, err := r.client.MGet(sk...).Result()
			err = r.convertError(err)
			if err != nil {
				return err
			}

			m := make(map[string]interface{})
			for i, k := range sk {
				m[k] = res[i]
			}

			resChan <- m
			return nil
		}
		fns = append(fns, fn)
	}

	err := Concurrency(fns, maxIdleSlotConns)
	if err != nil {
		return nil, err
	}

	results := make([]interface{}, 0)
	m := make(map[string]interface{})
	for len(resChan) > 0 {
		for k, v := range <-resChan {
			m[k] = v
		}
	}

	for _, k := range keys {
		results = append(results, m[k])
	}

	return results, nil
}

func (r *Redis) MSet(kvs map[string]interface{}) error {
	defer r.elapse(r.start(), "MSET", kvs)

	keys := make([]string, 0, len(kvs))
	m := make(map[string]string, len(kvs))
	for k, v := range kvs {
		val, err := r.String(v, nil)
		if err != nil {
			return err
		}

		keys = append(keys, k)
		m[k] = val
	}

	slots := r.groupKeysBySlot(keys)
	fns := []Fn{}

	for _, slotKeys := range slots {
		sk := slotKeys
		fn := func() error {
			args := make([]interface{}, 0, len(sk)*2)
			for _, k := range sk {
				args = append(args, k)
				args = append(args, m[k])
			}

			return r.convertError(r.client.MSet(args...).Err())
		}
		fns = append(fns, fn)
	}

	return Concurrency(fns, maxIdleSlotConns)
}

func (r *Redis) MSetNX(kvs map[string]interface{}) (bool, error) {
	defer r.elapse(r.start(), "MSETNX", kvs)

	keys := make([]string, 0, len(kvs))
	m := make(map[string]string, len(kvs))
	for k, v := range kvs {
		val, err := r.String(v, nil)
		if err != nil {
			return false, err
		}

		keys = append(keys, k)
		m[k] = val
	}

	slots := r.groupKeysBySlot(keys)
	resChan := make(chan bool, len(slots))
	defer func() {
		close(resChan)
	}()
	fns := []Fn{}

	for _, slotKeys := range slots {
		sk := slotKeys
		fn := func() error {
			args := make([]interface{}, 0, len(sk)*2)
			for _, k := range sk {
				args = append(args, k)
				args = append(args, m[k])
			}

			res, err := r.client.MSetNX(args...).Result()
			if err != nil {
				return r.convertError(err)
			}

			resChan <- res
			return nil
		}
		fns = append(fns, fn)
	}

	err := Concurrency(fns, maxIdleSlotConns)
	if err != nil {
		return false, err
	}

	result := true
	for len(resChan) > 0 {
		if !<-resChan {
			result = false
			break
		}
	}

	return result, nil
}

func (r *Redis) ExpireAt(key string, timestamp int64) (int64, error) {
	defer r.elapse(r.start(), "EXPIREAT", key, timestamp)

	result, err := r.client.ExpireAt(key, time.Unix(timestamp, 0)).Result()
	if err != nil {
		return 0, r.convertError(err)
	}

	if result {
		return 1, nil
	}

	return 0, nil
}

func (r *Redis) Del(keys ...string) (int64, error) {
	args := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}

	defer r.elapse(r.start(), "DEL", args...)

	result, err := r.client.Del(keys...).Result()
	if err != nil {
		return result, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) Incr(key string) (int64, error) {
	defer r.elapse(r.start(), "INCR", key)

	result, err := r.client.Incr(key).Result()
	if err != nil {
		return result, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) IncrBy(key string, delta int64) (int64, error) {
	defer r.elapse(r.start(), "INCRBY", key, delta)

	result, err := r.client.IncrBy(key, delta).Result()
	if err != nil {
		return result, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) Expire(key string, duration int64) (int64, error) {
	defer r.elapse(r.start(), "EXPIRE", key, duration)

	result, err := r.client.Expire(key, time.Duration(duration)*time.Second).Result()
	if err != nil {
		return 0, r.convertError(err)
	}

	if result {
		return 1, nil
	}

	return 0, nil
}

func (r *Redis) Exists(key string) (bool, error) {
	defer r.elapse(r.start(), "EXISTS", key)

	result, err := r.client.Exists(key).Result()
	if err != nil {
		return false, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) HGet(key string, field string) (interface{}, error) {
	defer r.elapse(r.start(), "HGET", key, field)

	result, err := r.client.HGet(key, field).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return []byte(result), nil
}

func (r *Redis) HLen(key string) (int64, error) {
	defer r.elapse(r.start(), "HLEN", key)

	result, err := r.client.HLen(key).Result()
	if err != nil {
		return result, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) HSet(key string, field string, val interface{}) error {
	defer r.elapse(r.start(), "HSET", key, field, val)

	value, err := r.String(val, nil)
	if err != nil && err != ErrNil {
		return err
	}

	_, err = r.client.HSet(key, field, value).Result()
	if err != nil {
		return r.convertError(err)
	}

	return nil
}

func (r *Redis) HDel(key string, fields ...string) (int64, error) {
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}

	defer r.elapse(r.start(), "HDEL", args...)

	result, err := r.client.HDel(key, fields...).Result()
	if err != nil {
		return 0, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) HClear(key string) error {
	_, err := r.Del(key)
	return err
}

func (r *Redis) HMGet(key string, fields ...string) (interface{}, error) {
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}

	defer r.elapse(r.start(), "HMGET", args...)

	if len(fields) == 0 {
		return nil, ErrNil
	}

	results, err := r.client.HMGet(key, fields...).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return results, nil
}

func (r *Redis) HMSet(key string, kvs ...interface{}) error {
	defer r.elapse(r.start(), "HMSET", kvs...)

	if len(kvs) == 0 {
		return nil
	}
	if len(kvs)%2 != 0 {
		return ErrWrongArgsNum
	}

	pairs := make(map[string]string)
	for i := 0; i < len(kvs)-1; i += 2 {
		kString, err := r.String(kvs[i], nil)
		if err != nil && err != ErrNil {
			return err
		}

		vString, err := r.String(kvs[i+1], nil)
		if err != nil && err != ErrNil {
			return err
		}

		pairs[kString] = vString
	}

	_, err := r.client.HMSet(key, pairs).Result()
	if err != nil {
		return r.convertError(err)
	}

	return nil
}

func (r *Redis) HExpire(key string, duration int64) error {
	_, err := r.Expire(key, duration)
	return err
}

func (r *Redis) HKeys(key string) ([]string, error) {
	defer r.elapse(r.start(), "HKEYS", key)

	results, err := r.client.HKeys(key).Result()
	if err != nil {
		return results, r.convertError(err)
	}

	return results, nil
}

func (r *Redis) HVals(key string) ([]interface{}, error) {
	defer r.elapse(r.start(), "HVALS", key)

	results, err := r.client.HVals(key).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	rs := make([]interface{}, 0, len(results))
	for _, result := range results {
		rs = append(rs, result)
	}

	return rs, nil
}

func (r *Redis) HGetAll(key string) (map[string]interface{}, error) {
	defer r.elapse(r.start(), "HGETALL", key)

	vals, err := r.client.HGetAll(key).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	result := make(map[string]interface{})
	for k, v := range vals {
		result[k] = v
	}

	return result, nil
}

func (r *Redis) HIncrBy(key, field string, delta int64) (int64, error) {
	defer r.elapse(r.start(), "HINCRBY", key, field, delta)

	result, err := r.client.HIncrBy(key, field, delta).Result()
	if err != nil {
		return result, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) ZAdd(key string, kvs ...interface{}) (int64, error) {
	args := make([]interface{}, 0, len(kvs)+1)
	args = append(args, key)
	args = append(args, kvs...)

	defer r.elapse(r.start(), "ZADD", args...)

	if len(kvs) == 0 {
		return 0, nil
	}
	if len(kvs)%2 != 0 {
		return 0, ErrWrongArgsNum
	}
	zs := make([]redis.Z, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		idx := i / 2
		score, err := r.Float64(kvs[i], nil)
		if err != nil && err != ErrNil {
			return 0, err
		}
		zs[idx].Score = score
		zs[idx].Member = kvs[i+1]
	}

	result, err := r.client.ZAdd(key, zs...).Result()
	if err != nil {
		return result, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) ZScore(key string, member string) (float64, error) {
	defer r.elapse(r.start(), "ZSCORE", key, member)
	result, err := r.client.ZScore(key, member).Result()
	if nil != err {
		return result, r.convertError(err)
	}
	return result, err
}

func (r *Redis) ZRem(key string, members ...string) (int64, error) {
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	ms := make([]interface{}, 0, len(members))
	for _, member := range members {
		args = append(args, member)
		ms = append(ms, member)
	}

	defer r.elapse(r.start(), "ZREM", args...)
	result, err := r.client.ZRem(key, ms...).Result()
	if err != nil {
		return result, r.convertError(err)
	}

	return result, err
}

func (r *Redis) ZClear(key string) error {
	_, err := r.Del(key)
	return err
}

func (r *Redis) ZExpire(key string, duration int64) error {
	_, err := r.Expire(key, duration)
	return err
}

func (r *Redis) ZRange(key string, min, max int64, withScores bool) (interface{}, error) {
	defer r.elapse(r.start(), "ZRANGE", key, min, max, withScores)

	results := make([]interface{}, 0)
	if withScores {
		zs, err := r.client.ZRangeWithScores(key, min, max).Result()
		if err != nil {
			return nil, r.convertError(err)
		}

		for _, z := range zs {
			results = append(results, z.Member, strconv.FormatFloat(z.Score, 'f', -1, 64))
		}
	} else {
		ms, err := r.client.ZRange(key, min, max).Result()
		if err != nil {
			return nil, r.convertError(err)
		}

		for _, m := range ms {
			results = append(results, m)
		}
	}

	return results, nil
}

func (r *Redis) ZRangeByScoreWithScore(key string, min, max int64) (map[string]int64, error) {
	panic("Not implemented!")
}

func (r *Redis) LRange(key string, start, stop int64) (interface{}, error) {
	defer r.elapse(r.start(), "LRANGE", key, start, stop)

	results, err := r.client.LRange(key, start, stop).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return results, nil
}

func (r *Redis) LSet(key string, index int, value interface{}) error {
	defer r.elapse(r.start(), "LSET", key, index, value)
	err := r.client.LSet(key, int64(index), value).Err()
	return r.convertError(err)
}

func (r *Redis) LLen(key string) (int64, error) {
	defer r.elapse(r.start(), "LLEN", key)

	result, err := r.client.LLen(key).Result()
	if err != nil {
		return result, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) LRem(key string, count int, value interface{}) (int, error) {
	defer r.elapse(r.start(), "LREM", key, count, value)

	val, _ := value.(string)
	result, err := r.client.LRem(key, int64(count), val).Result()
	if err != nil {
		return int(result), r.convertError(err)
	}

	return int(result), nil
}

func (r *Redis) TTl(key string) (int64, error) {
	defer r.elapse(r.start(), "TTL", key)

	duration, err := r.client.TTL(key).Result()
	if err != nil {
		return int64(duration.Seconds()), r.convertError(err)
	}

	return int64(duration.Seconds()), nil
}

func (r *Redis) LPop(key string) (interface{}, error) {
	defer r.elapse(r.start(), "LPOP", key)

	result, err := r.client.LPop(key).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) RPop(key string) (interface{}, error) {
	defer r.elapse(r.start(), "RPOP", key)

	result, err := r.client.RPop(key).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) BLPop(key string, timeout int) (interface{}, error) {
	defer r.elapse(r.start(), "BLPOP", key, timeout)

	result, err := r.client.BLPop(time.Duration(timeout)*time.Second, key).Result()
	if err != nil {
		// 兼容redis 2.x
		if err == redis.Nil {
			return nil, ErrNil
		}

		return nil, err
	}

	return result[1], nil
}

func (r *Redis) BRPop(key string, timeout int) (interface{}, error) {
	defer r.elapse(r.start(), "BRPOP", key, timeout)

	result, err := r.client.BRPop(time.Duration(timeout)*time.Second, key).Result()
	if err != nil {
		// 兼容redis 2.x
		if err == redis.Nil {
			return nil, ErrNil
		}

		return nil, r.convertError(err)
	}

	return result[1], nil
}

func (r *Redis) LPush(key string, value ...interface{}) error {
	args := make([]interface{}, 0, len(value)+1)
	args = append(args, key)
	args = append(args, value...)

	defer r.elapse(r.start(), "LPUSH", args...)

	vals := make([]string, 0, len(value))
	for _, v := range value {
		val, err := r.String(v, nil)
		if err != nil && err != ErrNil {
			return err
		}
		vals = append(vals, val)
	}

	_, err := r.client.LPush(key, value...).Result()
	if err != nil {
		return r.convertError(err)
	}

	return nil
}

func (r *Redis) RPush(key string, value ...interface{}) error {
	args := make([]interface{}, 0, len(value)+1)
	args = append(args, key)
	args = append(args, value...)

	defer r.elapse(r.start(), "RPUSH", args...)

	vals := make([]string, 0, len(value))
	for _, v := range value {
		val, err := r.String(v, nil)
		if err != nil && err != ErrNil {
			if err == ErrNil {
				log.Error("Rpush nil value. [key=%s, value=%v]", key, v)
			}
			return err
		}
		if val == "" {
			log.Error("Rpush nil value. [key=%s, value=%v]", key, v)
		}
		vals = append(vals, val)
	}

	_, err := r.client.RPush(key, value...).Result()
	if err != nil {
		return r.convertError(err)
	}

	return nil
}

// 为确保srcKey跟destKey映射到同一个slot，srcKey和destKey需要加上hash tag，如:{test}
func (r *Redis) BRPopLPush(srcKey string, destKey string, timeout int) (interface{}, error) {
	defer r.elapse(r.start(), "BRPOPLPUSH", srcKey, destKey, timeout)

	result, err := r.client.BRPopLPush(srcKey, destKey, time.Duration(timeout)*time.Second).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return result, nil
}

// 为确保srcKey跟destKey映射到同一个slot，srcKey和destKey需要加上hash tag，如:{test}
func (r *Redis) RPopLPush(srcKey string, destKey string) (interface{}, error) {
	defer r.elapse(r.start(), "RPOPLPUSH", srcKey, destKey)

	result, err := r.client.RPopLPush(srcKey, destKey).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) SAdd(key string, members ...interface{}) (int64, error) {
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	args = append(args, members...)

	defer r.elapse(r.start(), "SADD", args...)

	ms := make([]string, 0, len(members))
	for _, member := range members {
		m, err := r.String(member, nil)
		if err != nil && err != ErrNil {
			return 0, err
		}
		ms = append(ms, m)
	}

	result, err := r.client.SAdd(key, members...).Result()
	if err != nil {
		return result, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) SPop(key string) ([]byte, error) {
	defer r.elapse(r.start(), "SPop", key)

	result, err := r.client.SPop(key).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return []byte(result), nil
}

func (r *Redis) SIsMember(key string, member interface{}) (bool, error) {
	defer r.elapse(r.start(), "SISMEMBER", key, member)

	m, err := r.String(member, nil)
	if nil != err && err != ErrNil {
		return false, err
	}
	result, err := r.client.SIsMember(key, m).Result()
	if err != nil {
		return result, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) SRem(key string, members ...interface{}) (int64, error) {
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	args = append(args, members...)

	defer r.elapse(r.start(), "SREM", args...)

	ms := make([]string, 0, len(members))
	for _, member := range members {
		m, err := r.String(member, nil)
		if err != nil && err != ErrNil {
			return 0, err
		}
		ms = append(ms, m)
	}

	result, err := r.client.SRem(key, members...).Result()
	if err != nil {
		return result, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) SMembers(key string) ([]string, error) {
	defer r.elapse(r.start(), "SMEMBERS", key)

	result, err := r.client.SMembers(key).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) ScriptLoad(luaScript string) (interface{}, error) {
	defer r.elapse(r.start(), "SCRIPT LOAD", luaScript)

	result, err := r.client.ScriptLoad(luaScript).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) EvalSha(sha1 string, numberKeys int, keysArgs ...interface{}) (interface{}, error) {
	vals := make([]interface{}, 0, len(keysArgs)+2)
	vals = append(vals, sha1, numberKeys)
	vals = append(vals, keysArgs...)

	defer r.elapse(r.start(), "EVALSHA", vals...)

	keys := make([]string, 0, numberKeys)
	args := make([]string, 0, len(keysArgs)-numberKeys)

	for i, value := range keysArgs {
		val, err := r.String(value, nil)
		if err != nil && err != ErrNil {
			return nil, err
		}
		if i < numberKeys {
			keys = append(keys, val)
		} else {
			args = append(args, val)
		}
	}

	result, err := r.client.EvalSha(sha1, keys, args).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) Eval(luaScript string, numberKeys int, keysArgs ...interface{}) (interface{}, error) {
	vals := make([]interface{}, 0, len(keysArgs)+2)
	vals = append(vals, luaScript, numberKeys)
	vals = append(vals, keysArgs...)

	defer r.elapse(r.start(), "EVAL", vals...)

	keys := make([]string, 0, numberKeys)
	args := make([]string, 0, len(keysArgs)-numberKeys)

	for i, value := range keysArgs {
		val, err := r.String(value, nil)
		if err != nil && err != ErrNil {
			return nil, err
		}
		if i < numberKeys {
			keys = append(keys, val)
		} else {
			args = append(args, val)
		}
	}

	result, err := r.client.Eval(luaScript, keys, args).Result()
	if err != nil {
		return nil, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) GetBit(key string, offset int64) (int64, error) {
	defer r.elapse(r.start(), "GETBIT", key, offset)

	result, err := r.client.GetBit(key, offset).Result()
	if err != nil {
		return result, r.convertError(err)
	}

	return result, nil
}

func (r *Redis) SetBit(key string, offset uint32, value int) (int, error) {
	defer r.elapse(r.start(), "GETBIT", key, offset, value)

	result, err := r.client.SetBit(key, int64(offset), value).Result()
	return int(result), r.convertError(err)
}

// 新增或者更新一个地理位置
func (r *Redis) GeoAdd(key string, longitude, latitude float64, name string) error {
	defer r.elapse(r.start(), "GEOADD", key, longitude, latitude, name)

	location := &redis.GeoLocation{
		Name:      name,
		Longitude: longitude,
		Latitude:  latitude,
	}
	err := r.client.GeoAdd(key, location).Err()
	return r.convertError(err)
}

func (r *Redis) GeoPos(key string, member string) (*redis.GeoPos, error) {
	defer r.elapse(r.start(), "GeoPos", key, member)
	result, err := r.client.GeoPos(key, member).Result()
	if err != nil {
		return nil, r.convertError(err)
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result[0], nil
}

// 查询某个地理位置指定半径以内的地点
func (r *Redis) GeoRadius(key string, longitude, latitude float64, radius float64, count int) ([]redis.GeoLocation, error) {
	defer r.elapse(r.start(), "GeoRadius", key, longitude, latitude)

	query := &redis.GeoRadiusQuery{
		Radius:   radius,
		Unit:     "m",
		WithDist: true,
		Count:    count,
		Sort:     "ASC",
	}

	result, err := r.client.GeoRadius(key, longitude, latitude, query).Result()
	if err != nil {
		return nil, r.convertError(err)
	}
	return result, nil
}

// 查询某个用户指定半径以内的地点
func (r *Redis) GeoRadiusByMember(key string, member string, radius float64, count int) ([]redis.GeoLocation, error) {
	defer r.elapse(r.start(), "GeoRadius", key, member)

	query := &redis.GeoRadiusQuery{
		Radius:   radius,
		Unit:     "m",
		WithDist: true,
		Count:    count,
		Sort:     "ASC",
	}

	result, err := r.client.GeoRadiusByMember(key, member, query).Result()
	if err != nil {
		return nil, r.convertError(err)
	}
	return result, nil
}

func (r *Redis) convertError(err error) error {
	if err == redis.Nil {
		// 为了兼容redis 2.x,这里不返回 ErrNil，ErrNil在调用redis_cluster_reply函数时才返回
		return nil
	}

	return err
}

func (r *Redis) start() time.Time {
	return time.Now()
}

func (r *Redis) elapse(startTime time.Time, cmd string, args ...interface{}) {
	duration := time.Since(startTime)

	if r.withTimeout && duration > default_slow_time {
		log.Error(`[Redis][checkShowCmd] "it took %.2f sec" "addrs=%s|cmd=%s|args=%s|max_active=%d"`,
			duration.Seconds(), r.addrs, cmd, util.Join(args, ","), redisClusterMaxActive)
	}
}
