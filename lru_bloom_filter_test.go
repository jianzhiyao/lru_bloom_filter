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
		OnCacheMiss: func(key string, c chan<- []byte) {
			//todo set cache by persistent data
			c <- []byte{}
			close(c)
		},
		Persister: func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
		},
	})
	defer lruBloomFilter.Close()

	for i := 1; i <= 1000; i++ {
		key := fmt.Sprintf("%d", rand.Intn(11))
		go lruBloomFilter.Put(key, []byte{1})
	}
}

func TestLruBloomFilter2_Put(t *testing.T) {
	lruBloomFilter := New(LruBloomFilterConfig{
		LruCacheSize:      10,
		BloomFilterConfig: BloomFilterConfig{K: 5, M: uint(1024) * 1024},
		OnCacheMiss: func(key string, c chan<- []byte) {
			//todo set cache by persistent data
			c <- []byte{}
			close(c)
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
		OnCacheMiss: func(key string, c chan<- []byte) {
			//todo set cache by persistent data
			c <- []byte{}
			close(c)
		},
		Persister: func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
		},
	})

	var key string
	for i := 1; i <= 100; i++ {
		key = fmt.Sprintf("%d", i)
		go lruBloomFilter.Put(key, []byte{byte(i)})
	}
	for i := 1; i <= 100; i++ {
		go func() {
			key = fmt.Sprintf("%d", i)
			if !lruBloomFilter.Test(key, []byte{byte(i)}) {
				t.Errorf("fail")
			}
		}()
	}
}

func TestLruBloomFilter2_Test(t *testing.T) {
	lruBloomFilter := New(LruBloomFilterConfig{
		LruCacheSize:      1,
		BloomFilterConfig: BloomFilterConfig{K: 5, M: uint(1024) * 1024},
		OnCacheMiss: func(key string, c chan<- []byte) {
			//todo set cache by persistent data
			c <- []byte{}
			close(c)
		},
		Persister: func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
		},
	})

	var str string
	for i := 1; i <= 100000; i++ {
		go func() {
			str = fmt.Sprintf("%d", i)
			if lruBloomFilter.Test("key", []byte(str)) {
				t.Errorf("fail")
			}
			go lruBloomFilter.Put("key", []byte(str))
		}()
	}
}

func TestLruBloomFilter_UseStatus(t *testing.T) {
	var a sync.Map
	_,loaded :=a.LoadOrStore(1,1)
	fmt.Println("kkk",loaded)
	_,loaded =a.LoadOrStore(1,1)
	fmt.Println("kkk",loaded)
	_,loaded =a.LoadOrStore(2,1)
	fmt.Println("kkk",loaded)



}