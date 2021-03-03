// Copyright 2021 Airbus Defence and Space
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blockcache_test

import (
	"bytes"
	"testing"

	"github.com/airbusgeo/godal/internal/blockcache"
	"github.com/stretchr/testify/assert"
)

func bytea(i int) []byte {
	return []byte{byte(i)}
}

func TestCache(t *testing.T) {
	_, err := blockcache.NewCache(0)
	assert.Error(t, err)
	cache, _ := blockcache.NewCache(4)
	for i := 0; i < 4; i++ {
		cache.Add("foo", uint(i), bytea(i))
	}
	for i := 0; i < 4; i++ {
		b, ok := cache.Get("foo", uint(i))
		if !ok {
			t.Errorf("block %d not found in cache", i)
		}
		if !bytes.Equal(b, bytea(i)) {
			t.Errorf("expected %v, got %v", bytea(i), b)
		}
	}
	cache.Add("foo", 5, bytea(5))
	b, ok := cache.Get("foo", 5)
	if !ok {
		t.Errorf("block 5 not found in cache")
	}
	if !bytes.Equal(b, bytea(5)) {
		t.Errorf("expected %v, got %v", bytea(5), b)
	}
	purged := false
	for i := 0; i < 4; i++ {
		_, ok := cache.Get("foo", uint(i))
		if !ok {
			purged = true
		}
	}
	if !purged {
		t.Error("entry not purged")
	}
	cache.Add("foobar", 0, []byte("bar"))
	cache.Add("foobar", 1, []byte("bar"))
	cache.PurgeKey("foo")
	for i := 0; i < 5; i++ {
		_, ok := cache.Get("foo", uint(i))
		if ok {
			t.Error("cache not purged")
		}
	}
	bar, _ := cache.Get("foobar", 0)
	if !bytes.Equal(bar, []byte("bar")) {
		t.Error("foobar 0 purged")
	}
	bar, _ = cache.Get("foobar", 1)
	if !bytes.Equal(bar, []byte("bar")) {
		t.Error("foobar 1 purged")
	}
}
