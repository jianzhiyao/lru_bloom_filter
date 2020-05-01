package lru_bloom_filter

import (
	"bytes"
	"github.com/hashicorp/golang-lru"
	"github.com/jianzhiyao/lru_bloom_filter/lib"
	"github.com/willf/bloom"
	"log"
	"sync"
	"time"
)

type LruBloomFilter struct {
	cache             *lru.Cache
	onCacheMiss       func(k string, c chan<- []byte)
	mutex             sync.Mutex
	bloomFilterConfig *BloomFilterConfig

	persister         func(k interface{}, v interface{})
	persistCheckEvery time.Duration

	keyUseStatus sync.Map

	//close tag
	isClose bool
}
type LruBloomFilterConfig struct {
	LruCacheSize      int
	BloomFilterConfig BloomFilterConfig
	OnCacheMiss       func(key string, c chan<- []byte)
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

func (lbf *LruBloomFilter) checkCacheExist(key string) {
	lbf.mutex.Lock()
	defer lbf.mutex.Unlock()

	keyHash := lbf.keyHash(key)
	if !lbf.cache.Contains(keyHash) {
		ch := make(chan []byte)
		go lbf.onCacheMiss(key, ch)

		result := <-ch
		if len(result) > 0{
			lbf.cache.Add(keyHash, result)
		}
	}
}

func (lbf *LruBloomFilter) putWithoutLock(key string, b []byte) {
	keyHash := lbf.keyHash(key)
	cacheBytes := bytes.NewBuffer([]byte{0})
	if cacheResult, ok := lbf.cache.Get(keyHash); ok {
		cacheBytes = bytes.NewBuffer(cacheResult.([]byte))
	}

	bloomFilter := bloom.New(lbf.bloomFilterConfig.M, lbf.bloomFilterConfig.K)
	var err error
	if cacheBytes.Cap() > 0 {
		if _, err = bloomFilter.ReadFrom(cacheBytes); err != nil {
			log.Fatal(err)
		}
	}

	bloomFilter.TestAndAdd(b)
	if _, err = bloomFilter.WriteTo(cacheBytes); err != nil {
		log.Fatal(err)
	}
	if len(cacheBytes.Bytes()) > 0 {
		lbf.cache.Add(keyHash, cacheBytes.Bytes())
		lbf.keyUseStatus.Store(keyHash, 1)
	}
	//sign that key is updated
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
		bloomFilter := bloom.New(lbf.bloomFilterConfig.M, lbf.bloomFilterConfig.K)
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

func (lbf *LruBloomFilter) Close() {
	lbf.cache.Purge()
	lbf.cache = nil
	lbf.bloomFilterConfig = nil
	lbf.isClose = true
}

func (lbf *LruBloomFilter) initPersisterTick() {
	if lbf.persister != nil && lbf.persistCheckEvery > 0 {
		go lib.Every(lbf.persistCheckEvery, func(t time.Time) bool {

			lbf.mutex.Lock()
			defer lbf.mutex.Unlock()

			for _, key := range lbf.cache.Keys() {
				strKey := key
				_, loaded := lbf.keyUseStatus.LoadOrStore(strKey, 1)
				if loaded {
					if value, ok := lbf.cache.Get(key); ok {
						go lbf.persister(key, value)
					}
				}
				lbf.keyUseStatus.Delete(strKey)
			}

			return !lbf.isClose
		})
	}
}

func (lbf *LruBloomFilter) onEvict(key interface{}, value interface{}) {
	k := key.(string)
	keyHash := lbf.keyHash(k)

	if lbf.persister != nil {
		//sign that key is evicted
		lbf.keyUseStatus.Delete(keyHash)
		go lbf.persister(k, value)
	}
}

func New(config LruBloomFilterConfig) LruBloomFilter {
	lbf := LruBloomFilter{
		onCacheMiss:       config.OnCacheMiss,
		bloomFilterConfig: &config.BloomFilterConfig,
		persister:         config.Persister,
		isClose:           false,
		persistCheckEvery: config.PersistCheckEvery,
	}

	lruCache, _ := lru.NewWithEvict(config.LruCacheSize, lbf.onEvict)
	lbf.cache = lruCache
	lbf.initPersisterTick()
	return lbf
}
