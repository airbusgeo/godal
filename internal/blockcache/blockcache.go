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
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/vburenin/nsync"
)

// KeyReaderAt is the interface that wraps the basic ReadAt method for the specified key
//
// ReadAt reads len(p) bytes from the resource identified by key into p
// starting at offset off. It returns the number of bytes read (0 <= n <= len(p)) and
// any error encountered.
//
// When ReadAt returns n < len(p), it returns a non-nil error explaining why more bytes
// were not returned. In this respect, ReadAt is stricter than io.Read.
//
// Even if ReadAt returns n < len(p), it may use all of p as scratch space during the call.
// If some data is available but not len(p) bytes, ReadAt blocks until either all the data
// is available or an error occurs. In this respect ReadAt is different from io.Read.
//
// If the n = len(p) bytes returned by ReadAt are at the end of the input source, ReadAt
// may return either err == io.EOF or err == nil.
//
// If ReadAt is reading from an input source with a seek offset, ReadAt should not affect
// nor be affected by the underlying seek offset.
//
// Clients of ReadAt can execute parallel ReadAt calls on the same input source.
//
// Implementations must not retain p.
type KeyReaderAt interface {
	ReadAt(key string, p []byte, off int64) (int, error)
}

// Cacher is the interface that wraps block caching functionality
//
// Add inserts data to the cache for the given key and blockID.
//
// Get fetches the data for the given key and blockID. It returns
// the data and wether the data was found in the cache or not
//
// Purge empties the underlying cache for the given key
type Cacher interface {
	Add(key string, blockID uint, data []byte)
	Get(key string, blockID uint) ([]byte, bool)
	PurgeKey(key string)
	Purge()
}

// NamedOnceMutex is a locker on arbitrary lock names.
type NamedOnceMutex interface {
	//Lock tries to acquire a lock on a keyed resource. If the keyed resource is not already locked,
	//Lock aquires a lock to the resource and returns true. If the keyed resource is already locked,
	//Lock waits until the resource has been unlocked and returns false
	Lock(key interface{}) bool
	//Unlock a keyed resource. Should be called by a client whose call to Lock returned true once the
	//resource is ready for consumption by other clients
	Unlock(key interface{})
}

// BlockCache caches fixed-sized chunks of a KeyReaderAt, and exposes a KeyReaderAt
// that feeds primarily from its internal cache, ensuring that concurrent requests
// only result in a single call to the source reader.
type BlockCache struct {
	blockSize   int64
	blmu        NamedOnceMutex //*nsync.NamedOnceMutex
	cache       Cacher
	reader      KeyReaderAt
	splitRanges bool
}

func New(reader KeyReaderAt, cache Cacher, blockSize uint, split bool) *BlockCache {

	if blockSize == 0 {
		blockSize = 64 * 1024
	}
	return &BlockCache{
		blmu:        nsync.NewNamedOnceMutex(),
		cache:       cache,
		blockSize:   int64(blockSize),
		reader:      reader,
		splitRanges: split,
	}
}
func (b *BlockCache) SetLocker(mu NamedOnceMutex) {
	b.blmu = mu
}

func (b *BlockCache) PurgeKey(key string) {
	b.cache.PurgeKey(key)
}

func (b *BlockCache) Purge() {
	b.cache.Purge()
}

type blockRange struct {
	start int64
	end   int64
}

func min(n1, n2 int64) int64 {
	if n1 > n2 {
		return n2
	}
	return int64(n1)
}

func (b *BlockCache) getRange(key string, rng blockRange) ([][]byte, error) {
	//fmt.Printf("getrange [%d-%d]\n", rng.start, rng.end)
	blocks := make([][]byte, rng.end-rng.start+1)
	var err error
	if rng.start == rng.end {
		blocks[0], err = b.getBlock(key, int64(rng.start))
		return blocks, err
	}
	done := make(chan bool)
	defer close(done)
	for i := rng.start; i <= rng.end; i++ {
		go func(id int64) {
			blockID := b.blockKey(key, id)
			if b.blmu.Lock(blockID) {
				//unlock block once we've finished
				<-done
				b.blmu.Unlock(blockID)
			}
		}(int64(i))
	}
	buf := make([]byte, (rng.end-rng.start+1)*b.blockSize)
	n, err := b.reader.ReadAt(key, buf, rng.start*b.blockSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	left := int64(n)
	for bid := int64(0); bid <= rng.end-rng.start && left > 0; bid++ {
		ll := min(left, b.blockSize)
		blocks[bid] = make([]byte, ll)
		copy(blocks[bid], buf[bid*b.blockSize:bid*b.blockSize+ll])
		left -= ll
		b.cache.Add(key, uint(rng.start+bid), blocks[bid])
	}
	return blocks, nil
}

func (b *BlockCache) applyBlock(mu *sync.Mutex, block int64, data []byte, written []int, bufs [][]byte, offsets []int64) {
	if len(data) == 0 {
		return
	}
	blockStart := block * b.blockSize
	blockEnd := blockStart + int64(len(data))
	for ibuf := 0; ibuf < len(bufs); ibuf++ {
		//fmt.Printf("maybe apply block [%d-%d] to [%d-%d]\n", blockStart, blockEnd, offsets[ibuf], offsets[ibuf]+int64(len(bufs[ibuf])))
		if blockStart < offsets[ibuf]+int64(len(bufs[ibuf])) &&
			blockEnd > offsets[ibuf] {
			bufStart := int64(0)
			dataStart := int64(0)
			dataLen := int64(len(data))
			if blockStart < offsets[ibuf] {
				dataStart = offsets[ibuf] - blockStart
				dataLen -= dataStart
			} else {
				bufStart = blockStart - offsets[ibuf]
			}
			if trimright := blockEnd - (offsets[ibuf] + int64(len(bufs[ibuf]))); trimright > 0 {
				dataLen -= trimright
			}
			if dataLen > 0 {
				//fmt.Printf("apply block [%d-%d] to [%d-%d]\n", blockStart, blockEnd, offsets[ibuf], offsets[ibuf]+int64(len(bufs[ibuf])))
				//fmt.Printf("=>[%d:] from [%d:%d]\n", bufStart+offsets[ibuf], blockStart+dataStart, blockStart+dataStart+dataLen)
				mu.Lock()
				written[ibuf] += copy(bufs[ibuf][bufStart:], data[dataStart:dataStart+dataLen])
				mu.Unlock()
			}
		}
	}
}

func (b *BlockCache) ReadAtMulti(key string, bufs [][]byte, offsets []int64) ([]int, error) {
	blids := make(map[int64]bool)
	for ibuf := range bufs {
		zblock := offsets[ibuf] / b.blockSize
		lblock := (offsets[ibuf] + int64(len(bufs[ibuf])) - 1) / b.blockSize
		for ib := zblock; ib <= lblock; ib++ {
			blids[ib] = true
		}
	}
	written := make([]int, len(bufs))
	mu := &sync.Mutex{}

	var err error
	if b.splitRanges {
		wg := sync.WaitGroup{}
		for k := range blids {
			wg.Add(1)
			go func(bid int64) {
				defer wg.Done()
				bdata, berr := b.getBlock(key, bid)
				if berr != nil && err == nil {
					err = berr
					return
				}
				b.applyBlock(mu, bid, bdata, written, bufs, offsets)
			}(k)
		}
		wg.Wait()
	} else {
		blocks := make([]int64, 0)
		for k := range blids {
			bdata, ok := b.cache.Get(key, uint(k))
			if ok {
				b.applyBlock(mu, k, bdata, written, bufs, offsets)
			} else {
				blocks = append(blocks, k)
			}
		}
		if len(blocks) > 0 {
			sort.Slice(blocks, func(i, j int) bool {
				return blocks[i] < blocks[j]
			})
			wg := sync.WaitGroup{}
			rng := blockRange{start: blocks[0], end: blocks[0]}
			for k := 1; k < len(blocks); k++ {
				if blocks[k] != blocks[k-1]+1 {
					rng.end = blocks[k-1]
					wg.Add(1)
					//fmt.Printf("get // range [%d,%d]\n", rng.start, rng.end)
					go func(rng blockRange) {
						defer wg.Done()
						bblocks, berr := b.getRange(key, rng)
						if berr != nil && err == nil {
							err = berr
							return
						}
						for ib := range bblocks {
							b.applyBlock(mu, rng.start+int64(ib), bblocks[ib], written, bufs, offsets)
						}
					}(rng)
					rng.start = blocks[k]
					rng.end = blocks[k]
				} else {
					rng.end = blocks[k]
				}
			}

			//fmt.Printf("get range [%d,%d]\n", rng.start, rng.end)
			bblocks, berr := b.getRange(key, rng)
			if berr != nil && err == nil {
				err = berr
			} else {
				for ib := range bblocks {
					b.applyBlock(mu, rng.start+int64(ib), bblocks[ib], written, bufs, offsets)
				}
			}

			wg.Wait()
			if err != nil {
				return written, err
			}
		}
	}
	for i, buf := range bufs {
		if written[i] != len(buf) {
			err = io.EOF
		}
	}
	return written, err
}

func (b *BlockCache) ReadAt(key string, p []byte, off int64) (int, error) {
	written, err := b.ReadAtMulti(key, [][]byte{p}, []int64{off})
	return written[0], err
}

func (b *BlockCache) blockKey(key string, id int64) string {
	return fmt.Sprintf("%s-%d", key, id)
}

func (b *BlockCache) getBlock(key string, id int64) ([]byte, error) {
	blockData, ok := b.cache.Get(key, uint(id))
	if ok {
		return blockData, nil
	}
	blockID := b.blockKey(key, id)
	if b.blmu.Lock(blockID) {
		buf := make([]byte, b.blockSize)
		n, err := b.reader.ReadAt(key, buf, int64(id)*int64(b.blockSize))
		if err != nil && !errors.Is(err, io.EOF) {
			b.blmu.Unlock(blockID)
			return nil, err
		}
		if n > 0 {
			buf = buf[0:n]
			b.cache.Add(key, uint(id), buf)
		} else {
			buf = nil
			b.cache.Add(key, uint(id), buf)
		}
		b.blmu.Unlock(blockID)
		return buf, nil
	}
	//else (lock not acquired, recheck from cache)
	return b.getBlock(key, id)
}
