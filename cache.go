package main

import (
	"errors"
	"hash/fnv"
	//"sync"
	"time"
)

//var mutexCache = &sync.RWMutex{}

func (index Index) CacheGet(key string) (string, error) {
	//mutexCache.RLock()
	r := index.Cache[cacheKeyHash(key)]
	//mutexCache.RUnlock()
	if r.Val == "" {
		return "", errors.New("Cache: Value with this key not found.")
	} else {
		if r.Expire > int(time.Now().Unix()) {
			return r.Val, nil
		} else {
			return "", errors.New("Cache: Value with this key expired.")
		}
	}
}
func (index Index) CacheSet(key string, val string, expire int) bool {
	//mutexCache.Lock()
	index.Cache[cacheKeyHash(key)] = struct {
		Val    string
		Expire int
	}{
		val,
		int(time.Now().Unix()) + expire,
	}
	//mutexCache.Unlock()
	return true
}

func cacheKeyHash(key string) [8]byte {
	var keyHash []byte = make([]byte, 8)
	var keyHash8 [8]byte
	h := fnv.New64a()

	keyHash = []byte{}
	h.Write([]byte(key))
	keyHash = h.Sum(keyHash)
	copy(keyHash8[:], keyHash[:8])

	return keyHash8
}
