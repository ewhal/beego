// Copyright 2014 beego Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package redis for cache provider
//
// depend on github.com/garyburd/redigo/redis
//
// go install github.com/garyburd/redigo/redis
//
// Usage:
// import(
//   _ "github.com/astaxie/beego/cache/redis"
//   "github.com/astaxie/beego/cache"
// )
//
//  bm, err := cache.NewCache("redis", `{"conn":"127.0.0.1:11211"}`)
//
//  more docs http://beego.me/docs/module/cache.md
package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"

	"github.com/astaxie/beego/cache"
)

var (
	// DefaultKey the collection name of redis for cache adapter.
	DefaultKey = "beecacheRedis"
)

// Cache is Redis cache adapter.
type Cache struct {
	p        *redis.Client // redis connection pool
	conninfo string
	dbNum    int
	key      string
	password string
}

// NewRedisCache create new redis cache with default collection name.
func NewRedisCache() cache.Cache {
	return &Cache{key: DefaultKey}
}

// associate with config key.
func (rc *Cache) associate(originKey interface{}) string {
	return fmt.Sprintf("%s:%s", rc.key, originKey)
}

// Get cache from redis.
func (rc *Cache) Get(key string) interface{} {
	if err := rc.p.Get(key).Err(); err == nil {
		if v, err := rc.p.Get(key).Bytes(); err == nil {
			return v
		}
		return nil

	}
	return nil
}

// GetMulti get cache from redis.
func (rc *Cache) GetMulti(keys []string) []interface{} {
	if v, err := rc.p.MGet(strings.Join(keys, " ")).Result(); err == nil {
		return v
	}
	return nil
}

// Put put cache to redis.
func (rc *Cache) Put(key string, val interface{}, timeout time.Duration) error {
	return rc.p.Set(key, val, timeout).Err()
}

// Delete delete cache in redis.
func (rc *Cache) Delete(key string) error {
	return rc.p.Del(key).Err()
}

// IsExist check cache's existence in redis.
func (rc *Cache) IsExist(key string) bool {

	val, err := rc.p.Exists(key).Result()
	if err != nil {
		return false
	}
	if val == 1 {
		return true
	}
	return false
}

// Incr increase counter in redis.
func (rc *Cache) Incr(key string) error {
	return rc.p.Incr(key).Err()
}

// Decr decrease counter in redis.
func (rc *Cache) Decr(key string) error {
	return rc.p.Decr(key).Err()
}

// ClearAll clean all cache in redis. delete this redis collection.
func (rc *Cache) ClearAll() error {
	return rc.p.FlushAll().Err()
}

// StartAndGC start redis cache adapter.
// config is like {"key":"collection key","conn":"connection info","dbNum":"0"}
// the cache item in redis are stored forever,
// so no gc operation.
func (rc *Cache) StartAndGC(config string) error {
	var cf map[string]string
	json.Unmarshal([]byte(config), &cf)

	if _, ok := cf["key"]; !ok {
		cf["key"] = DefaultKey
	}
	if _, ok := cf["conn"]; !ok {
		return errors.New("config has no conn key")
	}
	if _, ok := cf["dbNum"]; !ok {
		cf["dbNum"] = "0"
	}
	if _, ok := cf["password"]; !ok {
		cf["password"] = ""
	}
	rc.key = cf["key"]
	rc.conninfo = cf["conn"]
	rc.dbNum, _ = strconv.Atoi(cf["dbNum"])
	rc.password = cf["password"]

	rc.connectInit()
	_, err := rc.p.Ping().Result()
	return err

}

// connect to redis.
func (rc *Cache) connectInit() {
	client := redis.NewClient(&redis.Options{
		Addr:        rc.conninfo,
		Password:    rc.password, // no password set
		DB:          rc.dbNum,    // use default DB
		IdleTimeout: 180 * time.Second,
	})
	rc.p = client
}

func init() {
	cache.Register("redis", NewRedisCache)
}
