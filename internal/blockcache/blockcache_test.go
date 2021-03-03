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
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/airbusgeo/godal/internal/blockcache"
)

type Reader struct {
	data []byte
}

var delay time.Duration

func (r Reader) ReadAt(key string, buf []byte, off int64) (int, error) {
	time.Sleep(delay)
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	if int(off) > len(r.data) {
		return 0, io.EOF
	}
	n := copy(buf, r.data[off:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

var rr Reader

func init() {
	data := make([]byte, 256*4)
	for i := byte(0); i <= 255; i++ {
		copy(data[int(i)*4:], []byte{i, i, i, i})
		if i == 255 {
			break
		}
	}
	rr = Reader{data}
}

func test(t *testing.T, bc *blockcache.BlockCache, buf []byte, offset int64, expectedLen int, expected []byte, experr error) {
	t.Helper()
	//t.Logf("read [%d-%d]", offset, offset+int64(len(buf)))
	r, err := bc.ReadAt("", buf, offset)
	if !errors.Is(err, experr) {
		t.Errorf("got error %v, expected %v", err, experr)
	}
	if r != expectedLen {
		t.Errorf("got %d bytes, expected %d", r, expectedLen)
	}
	if !bytes.Equal(buf[0:r], expected) {
		t.Errorf("got %v, expected %v", buf[0:r], expected)
	}

}

func TestBlockCache(t *testing.T) {
	for blockSize := 1; blockSize < 20; blockSize++ {
		for cacheSize := 1; cacheSize < 20; cacheSize++ {
			t.Logf("bs: %d, cs:%d", blockSize, cacheSize)
			testBlockCache(t, true, blockSize, cacheSize)
			testBlockCache(t, false, blockSize, cacheSize)
		}
	}
}

func testBlockCache(t *testing.T, split bool, blockSize int, numCachedBlocks int) {

	cache, _ := blockcache.NewCache(uint(numCachedBlocks))
	bc := blockcache.New(rr, cache, uint(blockSize), split)

	buf := make([]byte, 4)
	buf2 := make([]byte, 4)
	wg := sync.WaitGroup{}
	delay = 2 * time.Millisecond
	wg.Add(2)
	go func() {
		defer wg.Done()
		test(t, bc, buf, 0, 4, []byte{0, 0, 0, 0}, nil)
	}()
	go func() {
		defer wg.Done()
		test(t, bc, buf2, 0, 4, []byte{0, 0, 0, 0}, nil)
	}()
	wg.Wait()
	wg.Add(2)
	go func() {
		defer wg.Done()
		buf := make([]byte, 16)
		test(t, bc, buf, 63, 16, []byte{15, 16, 16, 16, 16, 17, 17, 17, 17, 18, 18, 18, 18, 19, 19, 19}, nil)
	}()
	go func() {
		defer wg.Done()
		buf := make([]byte, 16)
		test(t, bc, buf, 63, 16, []byte{15, 16, 16, 16, 16, 17, 17, 17, 17, 18, 18, 18, 18, 19, 19, 19}, nil)
	}()
	wg.Wait()
	delay = 0
	test(t, bc, buf, 2, 4, []byte{0, 0, 1, 1}, nil)
	test(t, bc, buf, 0, 4, []byte{0, 0, 0, 0}, nil)
	test(t, bc, buf, 2, 4, []byte{0, 0, 1, 1}, nil)
	buf = make([]byte, 8)
	test(t, bc, buf, 0, 8, []byte{0, 0, 0, 0, 1, 1, 1, 1}, nil)
	test(t, bc, buf, 2, 8, []byte{0, 0, 1, 1, 1, 1, 2, 2}, nil)
	test(t, bc, buf, 2, 8, []byte{0, 0, 1, 1, 1, 1, 2, 2}, nil)
	bc.Purge()
	test(t, bc, buf, 255*4, 4, []byte{255, 255, 255, 255}, io.EOF)
	test(t, bc, buf, 255*4-2, 6, []byte{254, 254, 255, 255, 255, 255}, io.EOF)
	test(t, bc, buf, 255*4-2, 6, []byte{254, 254, 255, 255, 255, 255}, io.EOF)
	test(t, bc, buf, 253*4, 8, []byte{253, 253, 253, 253, 254, 254, 254, 254}, nil)
	test(t, bc, buf, 255*4+2, 2, []byte{255, 255}, io.EOF)
	test(t, bc, buf, 256*4, 0, []byte{}, io.EOF)
	test(t, bc, buf, 256*4+2, 0, []byte{}, io.EOF) //outside bounds, but first block touches last data block
	test(t, bc, buf, 256*4+5, 0, []byte{}, io.EOF)
	buf = make([]byte, 12)
	test(t, bc, buf[0:4], 200*4, 4, []byte{200, 200, 200, 200}, nil)
	test(t, bc, buf, 200*4, 12, []byte{200, 200, 200, 200, 201, 201, 201, 201, 202, 202, 202, 202}, nil)
	test(t, bc, buf, 198*4, 12, []byte{198, 198, 198, 198, 199, 199, 199, 199, 200, 200, 200, 200}, nil)

	buf = make([]byte, 4)
	test(t, bc, buf, 0, 4, []byte{0, 0, 0, 0}, nil)
	test(t, bc, buf, 0, 4, []byte{0, 0, 0, 0}, nil)
	test(t, bc, buf, 2, 4, []byte{0, 0, 1, 1}, nil)
	test(t, bc, buf, 0, 4, []byte{0, 0, 0, 0}, nil)
	test(t, bc, buf, 2, 4, []byte{0, 0, 1, 1}, nil)
	buf = make([]byte, 8)
	test(t, bc, buf, 0, 8, []byte{0, 0, 0, 0, 1, 1, 1, 1}, nil)
	test(t, bc, buf, 2, 8, []byte{0, 0, 1, 1, 1, 1, 2, 2}, nil)
	test(t, bc, buf, 2, 8, []byte{0, 0, 1, 1, 1, 1, 2, 2}, nil)
	test(t, bc, buf, 255*4, 4, []byte{255, 255, 255, 255}, io.EOF)
	test(t, bc, buf, 255*4+2, 2, []byte{255, 255}, io.EOF)
	test(t, bc, buf, 256*4, 0, []byte{}, io.EOF)
	test(t, bc, buf, 256*4+2, 0, []byte{}, io.EOF) //outside bounds, but first block touches last data block
	test(t, bc, buf, 256*4+5, 0, []byte{}, io.EOF)
	bc.Purge()

	//read before and after an already cached block
	buf = make([]byte, blockSize*4)
	expx := make([]byte, blockSize*4)
	exp, _ := rr.ReadAt("", expx, int64(blockSize*3-blockSize/2))
	_, _ = bc.ReadAt("", buf[0:blockSize], int64(blockSize*3))
	test(t, bc, buf, int64(blockSize*3-blockSize/2), exp, expx, nil)

}
