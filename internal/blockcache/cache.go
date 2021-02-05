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

package blockcache

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru"
)

type Cache struct {
	c      *lru.Cache
	random string
}

var _ Cacher = &Cache{}

func NewCache(Entries uint) (*Cache, error) {
	c, err := lru.New(int(Entries))
	if err != nil {
		return nil, fmt.Errorf("lru.new: %w", err)
	}
	r := rand.New(rand.NewSource(time.Now().Unix()))
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, 5)
	for i := range b {
		b[i] = letterRunes[r.Intn(len(letterRunes))]
	}
	return &Cache{c: c, random: string(b)}, nil
}

func (cg *Cache) Add(key string, id uint, data []byte) {
	cg.c.Add(skey(key, cg.random, id), data)
}

func (cg *Cache) Get(key string, id uint) ([]byte, bool) {
	var cb interface{}
	var ok bool
	cb, ok = cg.c.Get(skey(key, cg.random, id))
	if !ok {
		return nil, ok
	}
	return cb.([]byte), ok
}

func (cg *Cache) PurgeKey(prefix string) {
	prefix = fmt.Sprintf("%s-%s-", prefix, cg.random)
	for _, k := range cg.c.Keys() {
		if strings.HasPrefix(k.(string), prefix) {
			cg.c.Remove(k)
		}
	}
}

func (cg *Cache) Purge() {
	cg.c.Purge()
}

func skey(key string, random string, id uint) string {
	return fmt.Sprintf("%s-%s-%d", key, random, id)
}
