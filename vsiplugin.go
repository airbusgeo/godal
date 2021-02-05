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

package godal

/*
#include <stdio.h>
#include "godal.h"
#cgo linux  pkg-config: gdal
*/
import "C"
import (
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

// VSIReader is the interface that should be returned by VSIKeyReader for a given
// key (i.e. filename)
//
// Size() is used as a probe to determine wether the given key exists, and should return
// an error if no such key exists. The actual file size may or may not be effectively used
// depending on the underlying GDAL driver opening the file
//
// VSIReader may also optionally implement VSIMultiReader which will be used (only?) by
// the GTiff driver when reading pixels. If not provided, this
// VSI implementation will concurrently call ReadAt([]byte,int64)
type VSIReader interface {
	io.ReaderAt
	Size() (uint64, error)
}

// VSIMultiReader is an optional interface that can be implemented by VSIReader that
// will be used (only?) by the GTiff driver when reading pixels. If not provided, this
// VSI implementation will concurrently call ReadAt([]byte,int64)
type VSIMultiReader interface {
	ReadAtMulti(bufs [][]byte, offs []int64) ([]int, error)
}

// VSIKeyReader is the interface that must be provided to RegisterVSIHandler. It
// should return a VSIReader for the given key.
//
// When registering a reader with
//  RegisterVSIHandler("scheme://",handler)
// calling Open("scheme://myfile.txt") will result in godal making calls to
//  VSIReader("myfile.txt")
type VSIKeyReader interface {
	VSIReader(key string) (VSIReader, error)
}

//export _gogdalSizeCallback
func _gogdalSizeCallback(key *C.char, errorString **C.char) C.longlong {
	//log.Printf("GetSize called")
	cbd := getGoGDALReader(key, errorString)
	if cbd == nil {
		return -1
	}
	size, err := cbd.Size()
	if err != nil {
		*errorString = C.CString(err.Error())
		return -1
	}
	return C.longlong(size)
}

//export _gogdalMultiReadCallback
func _gogdalMultiReadCallback(key *C.char, nRanges C.int, pocbuffers unsafe.Pointer, coffsets unsafe.Pointer, clengths unsafe.Pointer, errorString **C.char) C.int {
	if nRanges == 0 {
		return -1
	}
	cbd := getGoGDALReader(key, errorString)
	if cbd == nil {
		return -1
	}
	n := int(nRanges)
	cbuffers := (*[1 << 28]unsafe.Pointer)(unsafe.Pointer(pocbuffers))[:n:n]
	lengths := (*[1 << 28]C.size_t)(unsafe.Pointer(clengths))[:n:n]
	offsets := (*[1 << 28]C.ulonglong)(unsafe.Pointer(coffsets))[:n:n]

	buffers := make([][]byte, n)
	goffsets := make([]int64, n)
	ret := int64(0)
	for b := range buffers {
		l := int(lengths[b])
		buffers[b] = (*[1 << 28]byte)(unsafe.Pointer(cbuffers[b]))[:l:l]
		goffsets[b] = int64(offsets[b])
	}
	var err error
	if mcbd, ok := cbd.(VSIMultiReader); ok {
		_, err = mcbd.ReadAtMulti(buffers, goffsets)
		if err != nil && err != io.EOF {
			*errorString = C.CString(err.Error())
			ret = -1
		}
		return C.int(ret)
	}
	var wg sync.WaitGroup
	wg.Add(n)
	for b := range buffers {
		go func(bidx int) {
			defer wg.Done()
			rlen, err := cbd.ReadAt(buffers[bidx], goffsets[bidx])
			if err != nil && err != io.EOF {
				if *errorString == nil {
					*errorString = C.CString(err.Error())
				}
				atomic.StoreInt64(&ret, -1)
			}
			if rlen != int(lengths[bidx]) {
				if *errorString == nil {
					*errorString = C.CString(err.Error())
				}
				atomic.StoreInt64(&ret, -1)
			}
		}(b)
	}
	wg.Wait()
	return C.int(ret)
}

//export _gogdalReadCallback
func _gogdalReadCallback(key *C.char, buffer unsafe.Pointer, off C.size_t, clen C.size_t, errorString **C.char) C.size_t {
	if clen == 0 {
		return 0
	}

	l := int(clen)
	cbd := getGoGDALReader(key, errorString)
	if cbd == nil {
		return 0
	}
	slice := (*[1 << 28]byte)(buffer)[:l:l]
	rlen, err := cbd.ReadAt(slice, int64(off))
	if err != nil && err != io.EOF {
		*errorString = C.CString(err.Error())
	}
	return C.size_t(rlen)
}

var handlers map[string]VSIKeyReader

func getGoGDALReader(ckey *C.char, errorString **C.char) VSIReader {
	key := C.GoString(ckey)
	for prefix, handler := range handlers {
		if strings.HasPrefix(key, prefix) {
			hndl, err := handler.VSIReader(key[len(prefix):])
			if err != nil {
				*errorString = C.CString(err.Error())
				return nil
			}
			return hndl
		}
	}
	*errorString = C.CString("handler not registered for prefix")
	return nil
}

type vsiHandlerOptions struct {
	bufferSize, cacheSize C.size_t
}

// VSIHandlerOption is an option that can be passed to RegisterVSIHandler
type VSIHandlerOption func(v *vsiHandlerOptions)

// VSIHandlerBufferSize sets the size of the gdal-native block size used for caching. Must be positive,
// can be set to 0 to disable this behavior (not recommended).
//
// Defaults to 64Kb
func VSIHandlerBufferSize(s int) VSIHandlerOption {
	return func(o *vsiHandlerOptions) {
		o.bufferSize = C.size_t(s)
	}
}

// VSIHandlerCacheSize sets the total number of gdal-native bytes used as cache *per handle*.
// Defaults to 128Kb.
func VSIHandlerCacheSize(s int) VSIHandlerOption {
	return func(o *vsiHandlerOptions) {
		o.cacheSize = C.size_t(s)
	}
}

// RegisterVSIHandler registers keyReader on the given prefix.
// When registering a reader with
//  RegisterVSIHandler("scheme://",handler)
// calling Open("scheme://myfile.txt") will result in godal making calls to
//  VSIKeyReader("myfile.txt").ReadAt(buf,offset)
func RegisterVSIHandler(prefix string, keyReader VSIKeyReader, opts ...VSIHandlerOption) {
	opt := vsiHandlerOptions{
		bufferSize: 64 * 1024,
		cacheSize:  2 * 64 * 1024,
	}
	for _, o := range opts {
		o(&opt)
	}
	if handlers == nil {
		handlers = make(map[string]VSIKeyReader)
	}
	if handlers[prefix] != nil {
		panic("handler already registered on prefix")
	}
	handlers[prefix] = keyReader
	C.VSIInstallGoHandler(C.CString(prefix), opt.bufferSize, opt.cacheSize)
}
