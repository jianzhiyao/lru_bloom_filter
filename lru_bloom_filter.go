package lru_bloom_filter

import (
	"bytes"
	"github.com/hashicorp/golang-lru"
	"github.com/willf/bloom"
	"log"
	"sync"
)

type LruBloomFilter struct {
	cache             *lru.Cache
	onCacheMiss       func(key string, c chan<- []byte)
	mutex             sync.Mutex
	bloomFilterConfig *BloomFilterConfig
}
type BloomFilterConfig struct {
	m uint
	k uint
}

func (lbf *LruBloomFilter) keyHash(key string) string {
	//h := sha1.New()
	//	//h.Write([]byte(key))
	//	//bs := h.Sum(nil)
	//	//h.Reset()
	//	//
	//	//return fmt.Sprintf("%x", bs)
	return key
}

func (lbf *LruBloomFilter) checkCacheExist(key string) {
	lbf.mutex.Lock()
	defer lbf.mutex.Unlock()

	keyHash := lbf.keyHash(key)
	if !lbf.cache.Contains(keyHash) {
		ch := make(chan []byte)
		go lbf.onCacheMiss(key, ch)
		lbf.cache.Add(keyHash, <-ch)
	}
}

func (lbf *LruBloomFilter) putWithoutLock(key string, b []byte) {
	keyHash := lbf.keyHash(key)
	var cacheBytes *bytes.Buffer
	if cacheResult, ok := lbf.cache.Get(keyHash); ok {
		cacheBytes = bytes.NewBuffer(cacheResult.([]byte))
	} else {
		cacheBytes = bytes.NewBuffer([]byte{})
	}

	bloomFilter := bloom.New(lbf.bloomFilterConfig.m, lbf.bloomFilterConfig.k)
	var err error
	if _, err = bloomFilter.ReadFrom(cacheBytes); err != nil {
		log.Fatal(err)
	}
	bloomFilter.TestAndAdd(b)
	if _, err = bloomFilter.WriteTo(cacheBytes); err != nil {
		log.Fatal(err)
	}
	lbf.cache.Add(keyHash, cacheBytes.Bytes())
	bloomFilter.ClearAll()
	bloomFilter = nil
}

func (lbf *LruBloomFilter) Put(key string, b []byte) {
	lbf.checkCacheExist(key)

	lbf.mutex.Lock()
	defer lbf.mutex.Unlock()

	lbf.putWithoutLock(key, b)
}

func (lbf *LruBloomFilter) Test(key string, b []byte) bool {
	lbf.mutex.Lock()
	defer lbf.mutex.Unlock()
	return lbf.testWithoutLock(key, b)
}

func (lbf *LruBloomFilter) testWithoutLock(key string, b []byte) bool {
	keyHash := lbf.keyHash(key)
	if !lbf.cache.Contains(keyHash) {
		return false
	} else {
		var bb *bytes.Buffer
		if cacheResult, ok := lbf.cache.Get(keyHash); ok {
			bb = bytes.NewBuffer(cacheResult.([]byte))
		} else {
			bb = bytes.NewBuffer([]byte{})
		}
		bloomFilter := bloom.New(lbf.bloomFilterConfig.m, lbf.bloomFilterConfig.k)
		if _, err := bloomFilter.ReadFrom(bb); err != nil {
			log.Fatal(err)
			return false
		}

		return bloomFilter.Test(b)
	}
}

func (lbf *LruBloomFilter) TestAndPut(key string, b []byte) bool {
	lbf.checkCacheExist(key)

	lbf.mutex.Lock()
	defer lbf.mutex.Unlock()

	if !lbf.testWithoutLock(key, b) {
		lbf.putWithoutLock(key, b)
		return true
	} else {
		return false
	}

}

func (lbf *LruBloomFilter) Close(){
	lbf.cache.Purge()
	lbf.cache = nil
	lbf.bloomFilterConfig = nil
}

func New(lruCacheSize int, c BloomFilterConfig, onCacheMiss func(key string, c chan<- []byte), onEvict func(key interface{}, value interface{})) LruBloomFilter {
	lruCache, _ := lru.NewWithEvict(lruCacheSize, onEvict)
	return LruBloomFilter{
		cache:             lruCache,
		onCacheMiss:       onCacheMiss,
		bloomFilterConfig: &c,
	}
}
