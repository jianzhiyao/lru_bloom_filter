package lru_bloom_filter

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func TestLruBloomFilter_PutAndTest(t *testing.T) {
	lruBloomFilter := New(LruBloomFilterConfig{
		LruCacheSize:      10,
		BloomFilterConfig: BloomFilterConfig{K: 5, M: uint(1024) * 1024},
		OnCacheMiss: func(key string, ) []byte {
			//todo set cache by persistent data
			return nil
		},
		Persister: func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
		},
	})
	defer lruBloomFilter.Close()

	lruBloomFilter.Put("1", []byte{1})
	if !lruBloomFilter.Test("1", []byte{1}) {
		t.Fatalf("fail")
	}

}

func TestLruBloomFilter_Put1(t *testing.T) {
	lruBloomFilter := New(LruBloomFilterConfig{
		LruCacheSize:      10,
		BloomFilterConfig: BloomFilterConfig{K: 5, M: uint(1024) * 1024},
		OnCacheMiss: func(key string, ) []byte {
			//todo set cache by persistent data
			return nil
		},
		Persister: func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
		},
	})
	defer lruBloomFilter.Close()

	for i := 1; i <= 1000; i++ {
		key := fmt.Sprintf("%d", rand.Intn(11))

		lruBloomFilter.Put(key, []byte{1})
		if !lruBloomFilter.Test(key, []byte{1}) {
			t.Fatalf("fail")
		}
	}
}

func TestLruBloomFilter_Test1(t *testing.T) {
	lruBloomFilter := New(LruBloomFilterConfig{
		LruCacheSize:      100,
		BloomFilterConfig: BloomFilterConfig{K: 5, M: uint(1024) * 1024},
		OnCacheMiss: func(key string, ) []byte {
			//todo set cache by persistent data
			return nil
		},
		Persister: func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
		},
	})
	defer lruBloomFilter.Close()

	var key string
	for i := 1; i <= 100; i++ {
		key = fmt.Sprintf("%d", i)
		lruBloomFilter.Put(key, []byte{byte(i)})
	}
	for i := 1; i <= 100; i++ {
		key = fmt.Sprintf("%d", i)
		if !lruBloomFilter.Test(key, []byte{byte(i)}) {
			t.Fatalf("fail")
		}
	}

	for i := 2000; i <= 3000; i++ {
		key = fmt.Sprintf("%d", i)
		if lruBloomFilter.Test(key, []byte{byte(i)}) {
			t.Fatalf("fail")
		}
	}
}

func TestLruBloomFilter_UseStatus(t *testing.T) {
	var a sync.Map
	_, loaded := a.LoadOrStore(1, 1)
	if loaded {
		t.Fatalf("fail")
	}
	_, loaded = a.LoadOrStore(1, 1)
	if !loaded {
		t.Fatalf("fail")
	}
	_, loaded = a.LoadOrStore(2, 1)
	if loaded {
		t.Fatalf("fail")
	}
}

func TestLruBloomFilter_Flush(t *testing.T) {
	lruBloomFilter := New(LruBloomFilterConfig{
		LruCacheSize:      100,
		BloomFilterConfig: BloomFilterConfig{K: 5, M: uint(1024) * 1024},
		OnCacheMiss: func(key string, ) []byte {
			//todo set cache by persistent data
			return nil
		},
		Persister: func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
			if key != "1" {
				t.Fatalf("fail")
			}
		},
	})
	defer lruBloomFilter.Close()

	lruBloomFilter.Put("1", []byte{1})
}