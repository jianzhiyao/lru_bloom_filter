package lru_bloom_filter

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func TestLruBloomFilter1_Put(t *testing.T) {
	lruBloomFilter := New(LruBloomFilterConfig{
		LruCacheSize:      10,
		BloomFilterConfig: BloomFilterConfig{K: 5, M: uint(1024) * 1024},
		OnCacheMiss: func(key string, ) []byte {
			//todo set cache by persistent data
			return []byte{}
		},
		Persister: func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
		},
	})
	defer lruBloomFilter.Close()

	for i := 1; i <= 1000; i++ {
		key := fmt.Sprintf("%d", rand.Intn(11))
		lruBloomFilter.Put(key, []byte{1})
	}
}

func TestLruBloomFilter2_Put(t *testing.T) {
	lruBloomFilter := New(LruBloomFilterConfig{
		LruCacheSize:      10,
		BloomFilterConfig: BloomFilterConfig{K: 5, M: uint(1024) * 1024},
		OnCacheMiss: func(key string, ) []byte {
			//todo set cache by persistent data
			return []byte{}
		},
		Persister: func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
		},
	})

	for i := 1; i <= 1000; i++ {
		key := fmt.Sprintf("%d", rand.Intn(9))
		go lruBloomFilter.Put(key, []byte{1})
	}
}

func TestLruBloomFilter1_Test(t *testing.T) {
	lruBloomFilter := New(LruBloomFilterConfig{
		LruCacheSize:      100,
		BloomFilterConfig: BloomFilterConfig{K: 5, M: uint(1024) * 1024},
		OnCacheMiss: func(key string, ) []byte {
			//todo set cache by persistent data
			return []byte{}
		},
		Persister: func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
		},
	})

	var key string
	for i := 1; i <= 100; i++ {
		key = fmt.Sprintf("%d", i)
		lruBloomFilter.Put(key, []byte{byte(i)})
	}
	for i := 1; i <= 100; i++ {
		key = fmt.Sprintf("%d", i)
		if !lruBloomFilter.Test(key, []byte{byte(i)}) {
			t.Errorf("fail")
		}
	}
}

func TestLruBloomFilter_UseStatus(t *testing.T) {
	var a sync.Map
	_, loaded := a.LoadOrStore(1, 1)
	if loaded {
		t.Errorf("fail")
	}
	_, loaded = a.LoadOrStore(1, 1)
	if !loaded {
		t.Errorf("fail")
	}
	_, loaded = a.LoadOrStore(2, 1)
	if loaded {
		t.Errorf("fail")
	}

}
