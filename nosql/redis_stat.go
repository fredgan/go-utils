package nosql

import (
	"time"

	"github.com/garyburd/redigo/redis"
	sync2 "github.com/fredgan/go-utils/sync"
)

type redisStat struct {
	store     *RedisStore
	Doing     sync2.AtomicInt64
	Total     sync2.AtomicInt64
	Errors    sync2.AtomicInt32
	AvgTime   sync2.AtomicDuration
	MaxTime   sync2.AtomicDuration
	TotalTime sync2.AtomicDuration
}

var stats = make([]*redisStat, 0)

func GetRedisStats() []*redisStat {
	return stats
}

func newRedisStat(store *RedisStore) *redisStat {
	s := new(redisStat)
	s.store = store
	stats = append(stats, s)
	return s
}

func (s *redisStat) Host() string {
	return s.store.host
}

func (s *redisStat) Port() int {
	return s.store.port
}

func (s *redisStat) DB() int {
	return s.store.db
}

func (s *redisStat) ActiveCount() int {
	return s.store.pool.ActiveCount()
}

func (s *redisStat) MaxActive() int {
	return s.store.pool.MaxActive
}

func (s *redisStat) MaxIdle() int {
	return s.store.pool.MaxIdle
}

func (s *redisStat) onStart() {
	s.Doing.Add(1)
}

func (s *redisStat) onDone(duration time.Duration, err error) {
	s.Doing.Add(-1)
	s.Total.Add(1)
	if err != nil && err != redis.ErrNil {
		s.Errors.Add(1)
	}
	s.TotalTime.Add(duration)
	if s.MaxTime.Get() < duration {
		s.MaxTime.Set(duration)
	}
	s.AvgTime.Set(s.TotalTime.Get() / time.Duration(s.Total.Get()))
}
