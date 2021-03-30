package nosql

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestStoreInterface(t *testing.T) {
	Convey("store interface", t, func() {
		addrs := []string{"redis.cluster:7000", "redis.cluster:7001", "redis.cluster:7002", "redis.cluster:7003", "redis.cluster:7004", "redis.cluster:7005"}
		redisClient, err := NewRedisStore("redis.store", 6379, 1)
		So(err, ShouldBeNil)

		redisCluster, err := NewRedisCluster(addrs)
		So(err, ShouldBeNil)

		var _ Store = redisClient
		var _ Store = redisCluster
	})
}

func TestKV(t *testing.T) {
	Convey("key value", t, func() {
		addrs := []string{"redis.cluster:7000", "redis.cluster:7001", "redis.cluster:7002", "redis.cluster:7003", "redis.cluster:7004", "redis.cluster:7005"}
		redisClient, err := NewRedisStore("redis.store", 6379, 1)
		So(err, ShouldBeNil)

		redisCluster, err := NewRedisCluster(addrs)
		So(err, ShouldBeNil)

		Convey("Set", func() {
			key := "Set"

			err := redisClient.Set(key, "test")
			So(err, ShouldBeNil)

			err = redisCluster.Set(key, "test")
			So(err, ShouldBeNil)

			Reset(func() {
				redisClient.Del(key)
				redisCluster.Del(key)
			})
		})

		Convey("Get", func() {
			Convey("Get successfully", func() {
				key := "Get successfully"
				err := redisClient.Set(key, 1)
				So(err, ShouldBeNil)

				err = redisCluster.Set(key, 1)
				So(err, ShouldBeNil)

				clientVal, err := redisClient.Get(key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.Get(key)
				So(err, ShouldBeNil)

				So(string(clientVal.([]byte)), ShouldEqual, string(clusterVal.([]byte)))

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("Get fail", func() {
				key := "Get fail"

				_, err := redisClient.Get(key)
				So(err, ShouldBeNil)

				_, err = redisCluster.Get(key)
				So(err, ShouldBeNil)
			})
		})

		Convey("GetSet", func() {
			Convey("GetSet with value", func() {
				key := "GetSet with value"
				redisClient.Set(key, key)
				redisCluster.Set(key, key)

				clientVal, err := redisClient.GetSet(key, key+key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.GetSet(key, key+key)
				So(err, ShouldBeNil)

				So(string(clientVal.([]byte)), ShouldEqual, string(clusterVal.([]byte)))

				clientVal, _ = redisClient.Get(key)
				clusterVal, _ = redisCluster.Get(key)
				So(string(clientVal.([]byte)), ShouldEqual, string(clusterVal.([]byte)))

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("GetSet without value", func() {
				key := "GetSet without value"

				_, err := redisClient.GetSet(key, key)
				So(err, ShouldBeNil)

				_, err = redisCluster.GetSet(key, key)
				So(err, ShouldBeNil)

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})
		})

		Convey("SetNx", func() {
			Convey("SetNx with value", func() {
				key := "SetNx with value"
				redisClient.Set(key, key)
				redisCluster.Set(key, key)

				clientVal, err := redisClient.SetNx(key, key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.SetNx(key, key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("SetNx without value", func() {
				key := "SetNx without value"
				clientVal, err := redisClient.SetNx(key, key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.SetNx(key, key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)

				clientResult, _ := redisClient.Get(key)
				clusterResult, _ := redisCluster.Get(key)
				So(string(clientResult.([]byte)), ShouldEqual, string(clusterResult.([]byte)))

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})
		})

		Convey("SetEx", func() {
			key := "SetEx"
			timeout := int64(1)
			err := redisCluster.SetEx(key, key, timeout)
			So(err, ShouldBeNil)

			clusterVal, err := redisCluster.Get(key)
			So(err, ShouldBeNil)
			So(string(clusterVal.([]byte)), ShouldEqual, key)

			time.Sleep(time.Duration(timeout+1) * time.Second)
			_, err = redisCluster.Get(key)
			So(err, ShouldBeNil)
		})

		Convey("SetNxEx", func() {
			Convey("SetNxEx with value", func() {
				key := "SetNxEx with value"
				timeout := int64(1)

				redisClient.Set(key, key)
				redisCluster.Set(key, key)

				err := redisClient.SetNxEx(key, key, timeout)
				So(err, ShouldEqual, ErrNil)

				err = redisCluster.SetNxEx(key, key, timeout)
				So(err, ShouldEqual, ErrNil)

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("SetNxEx without value", func() {
				key := "SetNxEx with value"
				timeout := int64(1)

				err := redisClient.SetNxEx(key, key, timeout)
				So(err, ShouldBeNil)

				err = redisCluster.SetNxEx(key, key, timeout)
				So(err, ShouldBeNil)

				time.Sleep(time.Duration(timeout+1) * time.Second)

				_, err = redisCluster.Get(key)
				So(err, ShouldBeNil)
			})
		})

		Convey("MSet", func() {
			key := "MSet"

			err := redisClient.MSet(map[string]interface{}{
				"test:{test}": "123123",
				"hehe:{test}": 4545,
			})
			So(err, ShouldBeNil)

			err = redisCluster.MSet(map[string]interface{}{
				"test:{test}": "123123",
				"hehe:{test}": 4545,
			})
			So(err, ShouldBeNil)

			Reset(func() {
				redisClient.Del(key)
				redisCluster.Del(key)
			})
		})

		Convey("MSetNx", func() {
			Convey("MSetNx with value", func() {
				key := "MSetNx with value"
				redisClient.Set(key, key)
				redisCluster.Set(key, key)

				clientVal, err := redisClient.MSetNX(map[string]interface{}{
					"test:{test}": "123123",
					"hehe:{test}": 4545,
				})
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.MSetNX(map[string]interface{}{
					"test:{test}": "123123",
					"hehe:{test}": 4545,
				})
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("MSetNx without value", func() {
				key := "MSetNx without value"
				clientVal, err := redisClient.MSetNX(map[string]interface{}{
					"test:{test}": "123123",
					"hehe:{test}": 4545,
				})
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.MSetNX(map[string]interface{}{
					"test:{test}": "123123",
					"hehe:{test}": 4545,
				})
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)

				key = "test:{test}"
				clientResult, _ := redisClient.Get(key)
				clusterResult, _ := redisCluster.Get(key)
				Printf("\n\n%+v, %+v\n\n", clientResult, clusterResult)
				// So(string(clientResult.([]byte)), ShouldEqual, string(clusterResult.([]byte)))

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})
		})

		Convey("MGet", func() {
			Convey("HGet successfully", func() {
				key := "HGet successfully"
				err := redisClient.Set(key, 1)
				So(err, ShouldBeNil)

				err = redisCluster.Set(key, 1)
				So(err, ShouldBeNil)

				clientVal, err := redisClient.MGet(key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.MGet(key)
				So(err, ShouldBeNil)

				Printf("\n\n%+v, %+v\n\n", clientVal, clusterVal)
				// So(string(clientVal.([]byte)), ShouldEqual, string(clusterVal.([]byte)))

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("MGet fail", func() {
				key := "MGet fail"

				_, err := redisClient.MGet(key)
				So(err, ShouldBeNil)

				_, err = redisCluster.MGet(key)
				So(err, ShouldBeNil)
			})
		})

		Convey("ExpireAt", func() {
			key := "ExpireAt"
			expire := time.Now().Unix() + 1

			redisClient.Set(key, key)
			redisCluster.Set(key, key)

			clientVal, err := redisClient.ExpireAt(key, expire)
			So(err, ShouldBeNil)

			clusterVal, err := redisCluster.ExpireAt(key, expire)
			So(err, ShouldBeNil)

			So(clientVal, ShouldEqual, clusterVal)

			time.Sleep(2 * time.Second)
			_, err = redisCluster.Get(key)
			So(err, ShouldBeNil)
		})

		Convey("Del", func() {
			key := "Del"

			redisClient.Set(key, key)
			redisCluster.Set(key, key)

			clientVal, err := redisClient.Del(key)
			So(err, ShouldBeNil)

			clusterVal, err := redisCluster.Del(key)
			So(err, ShouldBeNil)

			So(clientVal, ShouldEqual, clusterVal)
		})

		Convey("Incr", func() {
			key := "Incr"

			clientVal, err := redisClient.Incr(key)
			So(err, ShouldBeNil)

			clusterVal, err := redisCluster.Incr(key)
			So(err, ShouldBeNil)

			So(clientVal, ShouldEqual, clusterVal)

			Reset(func() {
				redisClient.Del(key)
				redisCluster.Del(key)
			})
		})

		Convey("IncrBy", func() {
			key := "IncrBy"
			val := int64(10)

			clientVal, err := redisClient.IncrBy(key, val)
			So(err, ShouldBeNil)

			clusterVal, err := redisCluster.IncrBy(key, val)
			So(err, ShouldBeNil)

			So(clientVal, ShouldEqual, clusterVal)

			Reset(func() {
				redisClient.Del(key)
				redisCluster.Del(key)
			})
		})

		Convey("Expire", func() {
			key := "Expire"
			timeout := int64(1)

			redisClient.Set(key, key)
			redisCluster.Set(key, key)

			clientVal, err := redisClient.Expire(key, timeout)
			So(err, ShouldBeNil)

			clusterVal, err := redisCluster.Expire(key, timeout)
			So(err, ShouldBeNil)

			So(clientVal, ShouldEqual, clusterVal)

			time.Sleep(time.Duration(timeout+1) * time.Second)
			_, err = redisCluster.Get(key)
			So(err, ShouldBeNil)
		})

		Convey("SetBit", func() {
			key := "SetBit"

			redisClient.Set(key, key)
			redisCluster.Set(key, key)

			_, err := redisClient.SetBit(key, 0, 1)
			So(err, ShouldBeNil)

			_, err = redisCluster.SetBit(key, 0, 1)
			So(err, ShouldBeNil)

			Reset(func() {
				redisClient.Del(key)
				redisCluster.Del(key)
			})
		})

		Convey("GetBit", func() {
			key := "GetBit"

			redisClient.SetBit(key, 0, 1)
			redisCluster.SetBit(key, 0, 1)

			clientVal, err := redisClient.GetBit(key, 0)
			So(err, ShouldBeNil)

			clusterVal, err := redisCluster.GetBit(key, 0)
			So(err, ShouldBeNil)

			So(clientVal, ShouldEqual, clusterVal)
			So(clusterVal, ShouldEqual, 1)

			Reset(func() {
				redisClient.Del(key)
				redisCluster.Del(key)
			})
		})
	})
}

func TestHash(t *testing.T) {
	Convey("hash", t, func() {
		addrs := []string{"redis.cluster:7000", "redis.cluster:7001", "redis.cluster:7002", "redis.cluster:7003", "redis.cluster:7004", "redis.cluster:7005"}
		redisClient, err := NewRedisStore("redis.store", 6379, 1)
		So(err, ShouldBeNil)

		redisCluster, err := NewRedisCluster(addrs)
		So(err, ShouldBeNil)

		Convey("HSet", func() {
			key := "HSet"

			err := redisClient.HSet(key, key, key)
			So(err, ShouldBeNil)

			err = redisCluster.HSet(key, key, key)
			So(err, ShouldBeNil)

			Reset(func() {
				redisClient.Del(key)
				redisCluster.Del(key)
			})
		})

		Convey("HGet", func() {
			Convey("HGet with value", func() {
				key := "HGet with value"

				redisClient.HSet(key, key, key)
				redisCluster.HSet(key, key, key)

				clientVal, err := redisClient.HGet(key, key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.HGet(key, key)
				So(err, ShouldBeNil)

				So(string(clientVal.([]byte)), ShouldEqual, string(clusterVal.([]byte)))

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})
		})

		Convey("HLen", func() {
			Convey("HLen with value", func() {
				key := "HLen with value"

				redisClient.HSet(key, key, key)
				redisCluster.HSet(key, key, key)

				clientVal, err := redisClient.HLen(key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.HLen(key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("HLen without value", func() {
				key := "HLen without value"

				clientVal, err := redisClient.HLen(key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.HLen(key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)
			})
		})

		Convey("HDel", func() {
			Convey("HDel with value", func() {
				key := "HDel with value"

				redisClient.HSet(key, key, key)
				redisCluster.HSet(key, key, key)

				clientVal, err := redisClient.HDel(key, key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.HDel(key, key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)
			})

			Convey("HDel without value", func() {
				key := "HDel without value"

				clientVal, err := redisClient.HDel(key, key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.HDel(key, key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)
			})
		})

		Convey("HMGet", func() {
			Convey("HMGet with value", func() {
				key := "HMGet with value"

				err := redisClient.HMSet(key, key, key, key+key, key+key)
				So(err, ShouldBeNil)

				err = redisCluster.HMSet(key, key, key, key+key, key+key)
				So(err, ShouldBeNil)

				clientVals, err := redisClient.HMGet(key, key, key+key)
				So(err, ShouldBeNil)

				clusterVals, err := redisCluster.HMGet(key, key, key+key)
				So(err, ShouldBeNil)

				clients, ok := clientVals.([]interface{})
				So(ok, ShouldBeTrue)

				clusters, ok := clusterVals.([]interface{})
				So(ok, ShouldBeTrue)

				for _, client := range clients {
					shouldHave := false
					for _, cluster := range clusters {
						if string(client.([]byte)) == cluster.(string) {
							shouldHave = true
							break
						}
					}

					So(shouldHave, ShouldBeTrue)
				}

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("HMGet without value", func() {
				key := "HMGet without value"

				_, err := redisClient.HMGet(key, key, key+key)
				So(err, ShouldBeNil)

				_, err = redisCluster.HMGet(key, key, key+key)
				So(err, ShouldBeNil)
			})
		})

		Convey("HMSet", func() {
			key := "HMSet"

			err := redisClient.HMSet(key, key, key, key+key, key+key)
			So(err, ShouldBeNil)

			err = redisCluster.HMSet(key, key, key, key+key, key+key)
			So(err, ShouldBeNil)

			vals, err := redisCluster.HGetAll(key)
			So(err, ShouldBeNil)

			for k, v := range vals {
				So(k == key || k == key+key, ShouldBeTrue)
				val := v.(string)
				So(val == key || val == key+key, ShouldBeTrue)
			}

			Reset(func() {
				redisClient.Del(key)
				redisCluster.Del(key)
			})
		})

		Convey("HKeys", func() {
			Convey("HKeys with value", func() {
				key := "HKeys with value"

				redisClient.HSet(key, key, key)
				redisCluster.HSet(key, key, key)

				clientVals, err := redisClient.HKeys(key)
				So(err, ShouldBeNil)

				clusterVals, err := redisCluster.HKeys(key)
				So(err, ShouldBeNil)

				So(len(clientVals), ShouldEqual, len(clusterVals))

				for _, clientVal := range clientVals {
					shouldHave := false
					for _, clusterVal := range clusterVals {
						if clientVal == clusterVal {
							shouldHave = true
							break
						}
					}

					So(shouldHave, ShouldBeTrue)
				}

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("HKeys without value", func() {
				key := "HKeys without value"

				clientVals, err := redisClient.HKeys(key)
				So(err, ShouldBeNil)

				clusterVals, err := redisCluster.HKeys(key)
				So(err, ShouldBeNil)

				So(len(clientVals), ShouldEqual, 0)
				So(len(clusterVals), ShouldEqual, 0)
			})
		})

		Convey("HVals", func() {
			Convey("HVals with value", func() {
				key := "HVals with value"

				redisClient.HSet(key, key, key)
				redisCluster.HSet(key, key, key)

				clientVals, err := redisClient.HVals(key)
				So(err, ShouldBeNil)

				clusterVals, err := redisCluster.HVals(key)
				So(err, ShouldBeNil)

				So(len(clientVals), ShouldEqual, len(clusterVals))

				for _, clientVal := range clientVals {
					shouldHave := false
					for _, clusterVal := range clusterVals {
						if string(clientVal.([]byte)) == clusterVal.(string) {
							shouldHave = true
							break
						}
					}

					So(shouldHave, ShouldBeTrue)
				}

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("HVals without value", func() {
				key := "HVals without value"

				clientVals, err := redisClient.HVals(key)
				So(err, ShouldBeNil)

				clusterVals, err := redisCluster.HVals(key)
				So(err, ShouldBeNil)

				So(len(clientVals), ShouldEqual, 0)
				So(len(clusterVals), ShouldEqual, 0)
			})
		})

		Convey("HGetAll", func() {
			Convey("HGetAll with value", func() {
				key := "HGetAll with value"

				redisClient.HSet(key, key, key)
				redisCluster.HSet(key, key, key)

				clientVals, err := redisClient.HGetAll(key)
				So(err, ShouldBeNil)

				clusterVals, err := redisCluster.HGetAll(key)
				So(err, ShouldBeNil)

				So(len(clientVals), ShouldEqual, len(clusterVals))

				for clientKey, clientVal := range clientVals {
					clusterVal, ok := clientVals[clientKey]
					So(ok, ShouldBeTrue)
					So(string(clientVal.([]byte)), ShouldEqual, string(clusterVal.([]byte)))
				}

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("HGetAll without value", func() {
				key := "HGetAll without value"

				clientVals, err := redisClient.HGetAll(key)
				So(err, ShouldBeNil)

				clusterVals, err := redisCluster.HGetAll(key)
				So(err, ShouldBeNil)

				So(len(clientVals), ShouldEqual, len(clusterVals))
			})
		})

		Convey("HIncrBy", func() {
			key := "HIncrBy"
			num := int64(10)

			clientVal, err := redisClient.HIncrBy(key, key, num)
			So(err, ShouldBeNil)

			clusterVal, err := redisCluster.HIncrBy(key, key, num)
			So(err, ShouldBeNil)

			So(clientVal, ShouldEqual, clusterVal)

			Reset(func() {
				redisClient.Del(key)
				redisCluster.Del(key)
			})
		})
	})
}

func TestZSet(t *testing.T) {
	Convey("zset", t, func() {
		addrs := []string{"redis.cluster:7000", "redis.cluster:7001", "redis.cluster:7002", "redis.cluster:7003", "redis.cluster:7004", "redis.cluster:7005"}
		redisClient, err := NewRedisStore("redis.store", 6379, 1)
		So(err, ShouldBeNil)

		redisCluster, err := NewRedisCluster(addrs)
		So(err, ShouldBeNil)

		Convey("ZAdd", func() {
			key := "ZAdd"
			score := 1.1

			clientVal, err := redisClient.ZAdd(key, score, key)
			So(err, ShouldBeNil)

			clusterVal, err := redisCluster.ZAdd(key, score, key)
			So(err, ShouldBeNil)

			So(clientVal, ShouldEqual, clusterVal)

			Reset(func() {
				redisClient.Del(key)
				redisCluster.Del(key)
			})
		})

		Convey("ZRem", func() {
			Convey("ZRem with value", func() {
				key := "ZRem with value"
				score := 1.1

				redisClient.ZAdd(key, score, key)
				redisCluster.ZAdd(key, score, key)

				clientVal, err := redisClient.ZRem(key, key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.ZRem(key, key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)
			})

			Convey("ZRem without value", func() {
				key := "ZRem without value"

				_, err := redisClient.ZRem(key, key)
				So(err, ShouldBeNil)

				_, err = redisCluster.ZRem(key, key)
				So(err, ShouldBeNil)
			})
		})

		Convey("ZRange", func() {
			Convey("ZRange with value and score", func() {
				key := "ZRange with value and score"
				score := 1.1

				redisClient.ZAdd(key, score, key, score+score, key+key)
				redisCluster.ZAdd(key, score, key, score+score, key+key)

				clientVals, err := redisClient.ZRange(key, 0, -1, true)
				So(err, ShouldBeNil)

				clusterVals, err := redisCluster.ZRange(key, 0, -1, true)
				So(err, ShouldBeNil)

				clients, ok := clientVals.([]interface{})
				So(ok, ShouldBeTrue)

				clusters, ok := clusterVals.([]interface{})
				So(ok, ShouldBeTrue)

				for _, client := range clients {
					shouldHave := false
					clientStr := string(client.([]byte))
					for _, cluster := range clusters {
						clientFloat, err := strconv.ParseFloat(clientStr, 64)
						if err == nil {
							clusterFloat, err := strconv.ParseFloat(clientStr, 64)
							if err == nil {
								if math.Dim(clientFloat, clusterFloat) < 0.000001 {
									shouldHave = true
									break
								}
							}
						}

						if _, ok := cluster.(string); ok {
							if clientStr == string(cluster.(string)) {
								shouldHave = true
								break
							}
						}
					}
					So(shouldHave, ShouldBeTrue)
				}

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("ZRange with value and without score", func() {
				key := "ZRange with value and without score"
				score := 1.1

				redisClient.ZAdd(key, score, key, score+score, key+key)
				redisCluster.ZAdd(key, score, key, score+score, key+key)

				clientVals, err := redisClient.ZRange(key, 0, -1, false)
				So(err, ShouldBeNil)

				clusterVals, err := redisCluster.ZRange(key, 0, -1, false)
				So(err, ShouldBeNil)

				clients, ok := clientVals.([]interface{})
				So(ok, ShouldBeTrue)

				clusters, ok := clusterVals.([]interface{})
				So(ok, ShouldBeTrue)

				for _, client := range clients {
					shouldHave := false
					clientStr := string(client.([]byte))
					for _, cluster := range clusters {
						clientFloat, err := strconv.ParseFloat(clientStr, 64)
						if err == nil {
							clusterFloat, err := strconv.ParseFloat(clientStr, 64)
							if err == nil {
								if math.Dim(clientFloat, clusterFloat) < 0.000001 {
									shouldHave = true
									break
								}
							}
						}

						if _, ok := cluster.(string); ok {
							if clientStr == string(cluster.(string)) {
								shouldHave = true
								break
							}
						}
					}
					So(shouldHave, ShouldBeTrue)
				}

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("ZRange without valule", func() {
				key := "ZRange without valule"
				_, err := redisClient.ZRange(key, 0, -1, true)
				So(err, ShouldBeNil)

				_, err = redisCluster.ZRange(key, 0, -1, true)
				So(err, ShouldBeNil)
			})
		})

		Convey("ZRangeByScore", func() {
			Convey("ZRangeByScore with value", func() {
				key := "ZRangeByScore with value"
				score := 1.1

				redisClient.ZAdd(key, score, key, score+score, key+key)
				redisCluster.ZAdd(key, score, key, score+score, key+key)

				clientVals, err := redisClient.ZRangeByScoreWithScore(key, 0, -1)
				So(err, ShouldBeNil)

				clusterVals, err := redisCluster.ZRangeByScoreWithScore(key, 0, -1)
				So(err, ShouldBeNil)

				for clientKey, clientVal := range clientVals {
					clusterVal, ok := clusterVals[clientKey]
					So(ok, ShouldBeTrue)
					So(clientVal, ShouldEqual, clusterVal)
				}

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("ZRangeByScore without value", func() {
				key := "ZRangeByScore without value"
				_, err := redisClient.ZRangeByScoreWithScore(key, 0, -1)
				So(err, ShouldBeNil)

				_, err = redisCluster.ZRangeByScoreWithScore(key, 0, -1)
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestList(t *testing.T) {
	Convey("list", t, func() {
		addrs := []string{"redis.cluster:7000", "redis.cluster:7001", "redis.cluster:7002", "redis.cluster:7003", "redis.cluster:7004", "redis.cluster:7005"}
		redisClient, err := NewRedisStore("redis.store", 6379, 1)
		So(err, ShouldBeNil)

		redisCluster, err := NewRedisCluster(addrs)
		So(err, ShouldBeNil)

		Convey("LPush", func() {
			key := "LPush"

			err := redisClient.LPush(key, key, key)
			So(err, ShouldBeNil)

			err = redisCluster.LPush(key, key, key)
			So(err, ShouldBeNil)

			Reset(func() {
				redisClient.Del(key)
				redisCluster.Del(key)
			})
		})

		Convey("RPush", func() {
			key := "RPush"

			err := redisClient.RPush(key, key, key)
			So(err, ShouldBeNil)

			err = redisCluster.RPush(key, key, key)
			So(err, ShouldBeNil)

			Reset(func() {
				redisClient.Del(key)
				redisCluster.Del(key)
			})
		})

		Convey("LRange", func() {
			Convey("LRange with value", func() {
				key := "LRange with value"

				redisClient.LPush(key, key, key+key)
				redisCluster.LPush(key, key, key+key)

				clientVals, err := redisClient.LRange(key, 0, -1)
				So(err, ShouldBeNil)

				clusterVals, err := redisCluster.LRange(key, 0, -1)
				So(err, ShouldBeNil)

				clients, ok := clientVals.([]interface{})
				So(ok, ShouldBeTrue)

				clusters, ok := clusterVals.([]string)
				So(ok, ShouldBeTrue)

				for _, client := range clients {
					cv, ok := client.([]byte)
					So(ok, ShouldBeTrue)
					shouldHave := false

					for _, cluster := range clusters {
						if string(cv) == cluster {
							shouldHave = true
							break
						}
					}

					So(shouldHave, ShouldBeTrue)
				}

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("LRange without value", func() {
				key := "LRange without value"

				_, err := redisClient.LRange(key, 0, -1)
				So(err, ShouldBeNil)

				_, err = redisCluster.LRange(key, 0, -1)
				So(err, ShouldBeNil)
			})
		})

		Convey("LLen", func() {
			key := "LLen"

			clientVal, err := redisClient.LLen(key)
			So(err, ShouldBeNil)

			clusterVal, err := redisCluster.LLen(key)
			So(err, ShouldBeNil)

			So(clientVal, ShouldEqual, clusterVal)
		})

		Convey("LRem", func() {
			Convey("LRem with value", func() {
				key := "LRem with value"

				err := redisClient.LPush(key, key)
				So(err, ShouldBeNil)

				err = redisCluster.LPush(key, key)
				So(err, ShouldBeNil)

				clientVal, err := redisClient.LRem(key, 1, key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.LRem(key, 1, key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)
			})

			Convey("LRem without value", func() {
				key := "LRem without value"

				clientVal, err := redisClient.LRem(key, 1, key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.LRem(key, 1, key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)
			})
		})

		Convey("LPop", func() {
			Convey("LPop with value", func() {
				key := "LPop with value"

				err := redisClient.LPush(key, key)
				So(err, ShouldBeNil)

				err = redisCluster.LPush(key, key)
				So(err, ShouldBeNil)

				clientVal, err := redisClient.LPop(key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.LPop(key)
				So(err, ShouldBeNil)

				So(string(clientVal.([]byte)), ShouldEqual, clusterVal.(string))
			})

			Convey("LPop without value", func() {
				key := "LPop without value"

				_, err := redisClient.LPop(key)
				So(err, ShouldBeNil)

				_, err = redisCluster.LPop(key)
				So(err, ShouldBeNil)
			})
		})

		Convey("RPop", func() {
			Convey("RPop with value", func() {
				key := "RPop with value"

				err := redisClient.LPush(key, key)
				So(err, ShouldBeNil)

				err = redisCluster.LPush(key, key)
				So(err, ShouldBeNil)

				clientVal, err := redisClient.RPop(key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.RPop(key)
				So(err, ShouldBeNil)

				So(string(clientVal.([]byte)), ShouldEqual, clusterVal.(string))
			})

			Convey("RPop without value", func() {
				key := "RPop without value"

				_, err := redisClient.RPop(key)
				So(err, ShouldBeNil)

				_, err = redisCluster.RPop(key)
				So(err, ShouldBeNil)
			})
		})

		Convey("BLPop", func() {
			Convey("BLPop with value", func() {
				key := "BLPop with value"
				timeout := 1

				err := redisClient.LPush(key, key)
				So(err, ShouldBeNil)

				err = redisCluster.LPush(key, key)
				So(err, ShouldBeNil)

				clientVal, err := redisClient.BLPop(key, timeout)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.BLPop(key, timeout)
				So(err, ShouldBeNil)

				So(string(clientVal.([]byte)), ShouldEqual, clusterVal.(string))
			})

			Convey("BLPop without value", func() {
				key := "BLPop without value"
				timeout := 1

				_, err := redisClient.BLPop(key, timeout)
				So(err, ShouldEqual, ErrNil)

				_, err = redisCluster.BLPop(key, timeout)
				So(err, ShouldEqual, ErrNil)
			})
		})

		Convey("BRPop", func() {
			Convey("BRPop with value", func() {
				key := "BRPop with value"
				timeout := 1

				err := redisClient.LPush(key, key)
				So(err, ShouldBeNil)

				err = redisCluster.LPush(key, key)
				So(err, ShouldBeNil)

				clientVal, err := redisClient.BRPop(key, timeout)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.BRPop(key, timeout)
				So(err, ShouldBeNil)

				So(string(clientVal.([]byte)), ShouldEqual, clusterVal.(string))
			})

			Convey("BRPop without value", func() {
				key := "BRPop without value"
				timeout := 1

				_, err := redisClient.BRPop(key, timeout)
				So(err, ShouldEqual, ErrNil)

				_, err = redisCluster.BRPop(key, timeout)
				So(err, ShouldEqual, ErrNil)
			})
		})

		Convey("BRPopLPush", func() {
			Convey("BRPopLPush", func() {
				Convey("BRPopLPush with value", func() {
					key := "BRPopLPush with value"
					hashTag := fmt.Sprintf(":{%s}", key)
					timeout := 1

					err := redisClient.LPush(key+hashTag, key)
					So(err, ShouldBeNil)

					err = redisCluster.LPush(key+hashTag, key)
					So(err, ShouldBeNil)

					clientVal, err := redisClient.BRPopLPush(key+hashTag, key+key+hashTag, timeout)
					So(err, ShouldBeNil)

					clusterVal, err := redisCluster.BRPopLPush(key+hashTag, key+key+hashTag, timeout)
					So(err, ShouldBeNil)

					So(string(clientVal.([]byte)), ShouldEqual, clusterVal.(string))

					Reset(func() {
						redisClient.Del(key + key + hashTag)
						redisCluster.Del(key + key + hashTag)
					})
				})

				Convey("BRPopLPush without value", func() {
					key := "BRPopLPush without value"
					hashTag := fmt.Sprintf(":{%s}", key)
					timeout := 1

					_, err := redisClient.BRPopLPush(key, key+key+hashTag, timeout)
					So(err, ShouldBeNil)

					_, err = redisCluster.BRPopLPush(key, key+key+hashTag, timeout)
					So(err, ShouldBeNil)
				})
			})
		})

		Convey("RPopLPush", func() {
			Convey("RPopLPush", func() {
				Convey("RPopLPush with value", func() {
					key := "RPopLPush with value"
					hashTag := fmt.Sprintf(":{%s}", key)

					err := redisClient.LPush(key+hashTag, key)
					So(err, ShouldBeNil)

					err = redisCluster.LPush(key+hashTag, key)
					So(err, ShouldBeNil)

					clientVal, err := redisClient.RPopLPush(key+hashTag, key+key+hashTag)
					So(err, ShouldBeNil)

					clusterVal, err := redisCluster.RPopLPush(key+hashTag, key+key+hashTag)
					So(err, ShouldBeNil)

					So(string(clientVal.([]byte)), ShouldEqual, clusterVal.(string))

					Reset(func() {
						redisClient.Del(key + key + hashTag)
						redisCluster.Del(key + key + hashTag)
					})
				})

				Convey("RPopLPush without value", func() {
					key := "RPopLPush without value"
					hashTag := fmt.Sprintf(":{%s}", key)

					_, err := redisClient.RPopLPush(key+hashTag, key+key+hashTag)
					So(err, ShouldBeNil)

					_, err = redisCluster.RPopLPush(key+hashTag, key+key+hashTag)
					So(err, ShouldBeNil)
				})
			})
		})
	})
}

func TestSet(t *testing.T) {
	Convey("set", t, func() {
		addrs := []string{"redis.cluster:7000", "redis.cluster:7001", "redis.cluster:7002", "redis.cluster:7003", "redis.cluster:7004", "redis.cluster:7005"}
		redisClient, err := NewRedisStore("redis.store", 6379, 1)
		So(err, ShouldBeNil)

		redisCluster, err := NewRedisCluster(addrs)
		So(err, ShouldBeNil)

		Convey("SAdd", func() {
			key := "SAdd"

			clientVal, err := redisClient.SAdd(key, key)
			So(err, ShouldBeNil)

			clusterVal, err := redisCluster.SAdd(key, key)
			So(err, ShouldBeNil)

			So(clientVal, ShouldEqual, clusterVal)

			Reset(func() {
				redisClient.Del(key)
				redisCluster.Del(key)
			})
		})

		Convey("SPop", func() {
			Convey("SPop with value", func() {
				key := "SPop with value"

				_, err := redisClient.SAdd(key, key)
				So(err, ShouldBeNil)

				_, err = redisCluster.SAdd(key, key)
				So(err, ShouldBeNil)

				clientVal, err := redisClient.SPop(key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.SPop(key)
				So(err, ShouldBeNil)

				So(string(clusterVal), ShouldEqual, key)
				So(string(clientVal), ShouldEqual, string(clusterVal))
			})

			Convey("Spop without value", func() {
				key := "Spop without value"

				_, err := redisClient.SPop(key)
				So(err, ShouldEqual, ErrNil)

				_, err = redisCluster.SPop(key)
				So(err, ShouldEqual, ErrNil)
			})
		})

		Convey("SIsMember", func() {
			Convey("SIsMember with key and value", func() {
				key := "SIsMember with key and value"

				redisClient.SAdd(key, key)
				redisCluster.SAdd(key, key)

				clientVal, err := redisClient.SIsMember(key, key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.SIsMember(key, key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("SIsMember with key but without value", func() {
				key := "SIsMember with key but without value"

				redisClient.SAdd(key, key)
				redisCluster.SAdd(key, key)

				clientVal, err := redisClient.SIsMember(key, key+key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.SIsMember(key, key+key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)

				Reset(func() {
					redisClient.Del(key)
					redisCluster.Del(key)
				})
			})

			Convey("SIsMember without key or value", func() {
				key := "SIsMember without key or value"

				clientVal, err := redisClient.SIsMember(key, key+key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.SIsMember(key, key+key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)
			})
		})

		Convey("SRem", func() {
			Convey("SRem with value", func() {
				key := "SRem with value"

				redisClient.SAdd(key, key)
				redisCluster.SAdd(key, key)

				clientVal, err := redisClient.SRem(key, key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.SRem(key, key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)
			})

			Convey("SRem without value", func() {
				key := "SRem without value"

				clientVal, err := redisClient.SRem(key, key)
				So(err, ShouldBeNil)

				clusterVal, err := redisCluster.SRem(key, key)
				So(err, ShouldBeNil)

				So(clientVal, ShouldEqual, clusterVal)
			})
		})
	})
}

func TestLua(t *testing.T) {
	Convey("lua", t, func() {
		addrs := []string{"redis.cluster:7000", "redis.cluster:7001", "redis.cluster:7002", "redis.cluster:7003", "redis.cluster:7004", "redis.cluster:7005"}
		redisClient, err := NewRedisStore("redis.store", 6379, 1)
		So(err, ShouldBeNil)

		redisCluster, err := NewRedisCluster(addrs)
		So(err, ShouldBeNil)

		Convey("Eval", func() {
			key := "eval"
			luaScript := `return redis.call("GET", KEYS[1])`

			redisClient.Set(key, key)
			redisCluster.Set(key, key)

			clientVal, err := redisClient.Eval(luaScript, 1, key)
			So(err, ShouldBeNil)

			clusterVal, err := redisCluster.Eval(luaScript, 1, key)
			So(err, ShouldBeNil)

			So(clusterVal.(string), ShouldEqual, key)
			So(string(clientVal.([]byte)), ShouldEqual, clusterVal.(string))

			Reset(func() {
				_, err := redisClient.Del(key)
				So(err, ShouldBeNil)

				_, err = redisCluster.Del(key)
				So(err, ShouldBeNil)
			})
		})

		Convey("Script Load", func() {
			luaScript := `return redis.call("GET", KEYS[1])`

			clientSha, err := redisClient.ScriptLoad(luaScript)
			So(err, ShouldBeNil)

			clusterSha, err := redisCluster.ScriptLoad(luaScript)
			So(err, ShouldBeNil)

			So(string(clientSha.([]byte)), ShouldEqual, clusterSha.(string))
		})

		Convey("EvalSha", func() {
			key := "EvalSha"
			luaScript := `return redis.call("GET", KEYS[1])`

			redisClient.Set(key, key)
			redisCluster.Set(key, key)

			clientSha, err := redisClient.ScriptLoad(luaScript)
			So(err, ShouldBeNil)

			clusterSha, err := redisCluster.ScriptLoad(luaScript)
			So(err, ShouldBeNil)

			clientVal, err := redisClient.EvalSha(string(clientSha.([]byte)), 1, key)
			So(err, ShouldBeNil)

			clusterVal, err := redisCluster.EvalSha(clusterSha.(string), 1, key)
			So(err, ShouldBeNil)

			So(clusterVal.(string), ShouldEqual, key)
			So(string(clientVal.([]byte)), ShouldEqual, clusterVal.(string))

			Reset(func() {
				_, err := redisClient.Del(key)
				So(err, ShouldBeNil)

				_, err = redisCluster.Del(key)
				So(err, ShouldBeNil)
			})
		})
	})
}
