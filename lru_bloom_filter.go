package lru_bloom_filter

import (
	"bytes"
	"github.com/hashicorp/golang-lru"
	"github.com/jianzhiyao/lru_bloom_filter/lib"
	"github.com/willf/bloom"
	"sync"
	"time"
)

type LruBloomFilter struct {
	cache             *lru.Cache
	onCacheMiss       func(k string) []byte
	mutex             sync.Mutex
	bloomFilterConfig *BloomFilterConfig

	persister         func(k interface{}, v interface{})
	persistCheckEvery time.Duration

	keyUseStatus map[string]byte

	//close tag
	isClose bool
}
type LruBloomFilterConfig struct {
	LruCacheSize      int
	BloomFilterConfig BloomFilterConfig
	OnCacheMiss       func(key string) []byte
	Persister         func(key interface{}, value interface{})
	PersistCheckEvery time.Duration
}
type BloomFilterConfig struct {
	M uint
	K uint
}

func (lbf *LruBloomFilter) keyHash(key string) string {
	return key
}

func (lbf *LruBloomFilter) checkCacheExistWithoutLock(key string) {
	keyHash := lbf.keyHash(key)
	if !lbf.cache.Contains(keyHash) {
		ch := make(chan []byte)
		go func() {
			ch <- lbf.onCacheMiss(key)
		}()

		result := <-ch
		if len(result) > 0 {
			lbf.cache.Add(keyHash, result)
		}
	}
}

func (lbf *LruBloomFilter) putWithoutLock(key string, b []byte) {
	//检查key存在
	lbf.checkCacheExistWithoutLock(key)

	keyHash := lbf.keyHash(key)

	var cacheBytes bytes.Buffer
	if cacheResult, _ := lbf.cache.Get(keyHash); cacheResult != nil {
		_, _ = cacheBytes.Read(cacheResult.([]byte))
	}

	bloomFilter := bloom.New(lbf.bloomFilterConfig.M, lbf.bloomFilterConfig.K)
	_, _ = bloomFilter.ReadFrom(&cacheBytes)

	bloomFilter.TestAndAdd(b)
	_, _ = bloomFilter.WriteTo(&cacheBytes)
	if cacheBytes.Cap() > 0 {
		lbf.cache.Add(keyHash, cacheBytes.Bytes())
		lbf.keyUseStatus[keyHash] = byte(1)
	}
	//sign that key is updated
	bloomFilter.ClearAll()
	bloomFilter = nil
}

func (lbf *LruBloomFilter) Put(key string, b []byte) {
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
	//检查key存在
	lbf.checkCacheExistWithoutLock(key)

	keyHash := lbf.keyHash(key)

	var bb bytes.Buffer
	if cacheResult, _ := lbf.cache.Get(keyHash); cacheResult != nil {
		bb = *bytes.NewBuffer(cacheResult.([]byte))
	}

	bloomFilter := bloom.New(lbf.bloomFilterConfig.M, lbf.bloomFilterConfig.K)
	_, _ = bloomFilter.ReadFrom(&bb)

	return bloomFilter.Test(b)
}

func (lbf *LruBloomFilter) TestAndPut(key string, b []byte) bool {
	lbf.mutex.Lock()
	defer lbf.mutex.Unlock()

	if !lbf.testWithoutLock(key, b) {
		lbf.putWithoutLock(key, b)
		return true
	} else {
		return false
	}

}

func (lbf *LruBloomFilter) Close() {
	//关闭之前 Flush 存盘
	lbf.Flush()

	lbf.mutex.Lock()
	defer lbf.mutex.Unlock()

	lbf.cache.Purge()
	lbf.cache = nil
	lbf.keyUseStatus = nil
	lbf.bloomFilterConfig = nil
	lbf.isClose = true
}

func (lbf *LruBloomFilter) initPersisterTick() {
	if lbf.persister != nil && lbf.persistCheckEvery > 0 {
		go lib.Every(lbf.persistCheckEvery, func(t time.Time) bool {
			lbf.Flush()
			return !lbf.isClose
		})
	}
}

func (lbf *LruBloomFilter) Flush() {
	lbf.mutex.Lock()
	defer lbf.mutex.Unlock()

	for _, key := range lbf.cache.Keys() {
		keyHash := key.(string)
		_, ok := lbf.keyUseStatus[keyHash]
		if ok {
			if value, ok := lbf.cache.Get(key); ok {
				go lbf.persister(key, value)
			}
		}
		delete(lbf.keyUseStatus, keyHash)
	}
}

func (lbf *LruBloomFilter) onEvict(key interface{}, value interface{}) {
	lbf.mutex.Lock()
	defer lbf.mutex.Unlock()

	k := key.(string)
	keyHash := lbf.keyHash(k)

	if lbf.persister != nil {
		//sign that key is evicted
		go lbf.persister(k, value)
		delete(lbf.keyUseStatus, keyHash)
	}
}

func New(config LruBloomFilterConfig) LruBloomFilter {
	lbf := LruBloomFilter{
		onCacheMiss:       config.OnCacheMiss,
		bloomFilterConfig: &config.BloomFilterConfig,
		persister:         config.Persister,
		isClose:           false,
		persistCheckEvery: config.PersistCheckEvery,
		keyUseStatus:      make(map[string]byte),
	}

	lruCache, _ := lru.NewWithEvict(config.LruCacheSize, lbf.onEvict)
	lbf.cache = lruCache
	lbf.initPersisterTick()
	return lbf
}
