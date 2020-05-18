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
			lbf.cache.Add(keyHash, string(result))
		}
	}
}

func (lbf *LruBloomFilter) putWithoutLock(key string, b []byte) {
	keyHash := lbf.keyHash(key)

	var cacheBytes *bytes.Buffer
	if cacheResult, ok := lbf.cache.Get(keyHash); ok {
		cacheBytes = bytes.NewBufferString(cacheResult.(string))
	} else {
		cacheBytes = bytes.NewBuffer([]byte{})
	}

	bloomFilter := bloom.New(lbf.bloomFilterConfig.M, lbf.bloomFilterConfig.K)
	var err error
	if cacheBytes.Cap() > 0 {
		if _, err = bloomFilter.ReadFrom(cacheBytes); err != nil {
			panic(err)
		}
	}

	bloomFilter.TestAndAdd(b)
	if _, err = bloomFilter.WriteTo(cacheBytes); err != nil {
		panic(err)
	}
	if cacheBytes.Cap() > 0 {
		lbf.cache.Add(keyHash, cacheBytes.String())
		lbf.keyUseStatus[keyHash] = byte(1)
	}
	//sign that key is updated
	bloomFilter.ClearAll()
	bloomFilter = nil
}

func (lbf *LruBloomFilter) Put(key string, b []byte) {
	lbf.mutex.Lock()
	defer lbf.mutex.Unlock()

	//检查key存在 & 初始化相关
	lbf.checkCacheExistWithoutLock(key)
	lbf.putWithoutLock(key, b)
}

func (lbf *LruBloomFilter) Test(key string, b []byte) bool {
	lbf.mutex.Lock()
	defer lbf.mutex.Unlock()

	//检查key存在
	lbf.checkCacheExistWithoutLock(key)
	return lbf.testWithoutLock(key, b)
}

func (lbf *LruBloomFilter) testWithoutLock(key string, b []byte) bool {
	keyHash := lbf.keyHash(key)
	if !lbf.cache.Contains(keyHash) {
		return false
	} else {
		var bb *bytes.Buffer
		if cacheResult, ok := lbf.cache.Get(keyHash); ok {
			bb = bytes.NewBufferString(cacheResult.(string))
		} else {
			bb = bytes.NewBuffer([]byte{})
		}
		bloomFilter := bloom.New(lbf.bloomFilterConfig.M, lbf.bloomFilterConfig.K)
		if _, err := bloomFilter.ReadFrom(bb); err != nil {
			panic(err)
		}

		return bloomFilter.Test(b)
	}
}

func (lbf *LruBloomFilter) TestAndPut(key string, b []byte) bool {
	lbf.mutex.Lock()
	defer lbf.mutex.Unlock()

	//检查key存在
	lbf.checkCacheExistWithoutLock(key)

	if !lbf.testWithoutLock(key, b) {
		lbf.putWithoutLock(key, b)
		return true
	} else {
		return false
	}

}

func (lbf *LruBloomFilter) Close() {
	lbf.cache.Purge()
	lbf.cache = nil
	lbf.keyUseStatus = nil
	lbf.bloomFilterConfig = nil
	lbf.isClose = true
}

func (lbf *LruBloomFilter) initPersisterTick() {
	if lbf.persister != nil && lbf.persistCheckEvery > 0 {
		go lib.Every(lbf.persistCheckEvery, func(t time.Time) bool {

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

			return !lbf.isClose
		})
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
