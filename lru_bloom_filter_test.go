package lru_bloom_filter

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestLruBloomFilter1_Put(t *testing.T) {
	lruBloomFilter := New(
		10,
		BloomFilterConfig{k: 5, m: uint(1024) * 1024},
		func(key string, c chan<- []byte) {
			//todo set cache by persistent data
			c <- []byte{}
			close(c)
		},
		func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
		})
	defer lruBloomFilter.Close()

	for i := 1; i <= 1000; i++ {
		key := fmt.Sprintf("%d", rand.Intn(11))
		go lruBloomFilter.Put(key, []byte{1})
	}
}

func TestLruBloomFilter2_Put(t *testing.T) {
	lruBloomFilter := New(
		10,
		BloomFilterConfig{k: 5, m: uint(1024) * 1024},
		func(key string, c chan<- []byte) {
			//todo set cache by persistent data
			c <- []byte{}
			close(c)
		},
		func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
		})
	defer lruBloomFilter.Close()

	for i := 1; i <= 1000; i++ {
		key := fmt.Sprintf("%d", rand.Intn(9))
		go lruBloomFilter.Put(key, []byte{1})
	}
}

func TestLruBloomFilter1_Test(t *testing.T) {
	lruBloomFilter := New(
		100,
		BloomFilterConfig{k: 5, m: uint(1024) * 1024},
		func(key string, c chan<- []byte) {
			//todo set cache by persistent data
			c <- []byte{}
			close(c)
		},
		func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
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
	lruBloomFilter := New(
		1,
		BloomFilterConfig{k: 5, m: uint(1024) * 1024},
		func(key string, c chan<- []byte) {
			//todo set cache by persistent data
			c <- []byte{}
			close(c)
		},
		func(key interface{}, value interface{}) {
			//todo persistent data after cache retirement
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
