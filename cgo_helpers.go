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

//#include <stdlib.h>
import "C"
import "unsafe"

func cIntArray(in []int) *C.int {
	ret := make([]C.int, len(in))
	for i := range in {
		ret[i] = C.int(in[i])
	}
	return (*C.int)(unsafe.Pointer(&ret[0]))
}

func cDoubleArray(in []float64) *C.double {
	ret := make([]C.double, len(in))
	for i := range in {
		ret[i] = C.double(in[i])
	}
	return (*C.double)(unsafe.Pointer(&ret[0]))
}

type cStringArray []*C.char

func (ca cStringArray) free() {
	for _, str := range ca {
		C.free(unsafe.Pointer(str))
	}
}

func (ca cStringArray) cPointer() **C.char {
	if len(ca) <= 1 { //nil terminated, must be at least len==2 to be not empty
		return nil
	}
	return (**C.char)(unsafe.Pointer(&ca[0]))
}

func sliceToCStringArray(in []string) cStringArray {
	if len(in) > 0 {
		arr := make([]*C.char, len(in)+1)
		for i := range in {
			arr[i] = C.CString(in[i])
		}
		arr[len(in)] = nil
		return arr
	}
	return nil
}

func cStringArrayToSlice(in **C.char) []string {
	if in == nil {
		return nil
	}
	//https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
	cStrs := (*[1 << 30]*C.char)(unsafe.Pointer(in))
	i := 0
	ret := []string{}
	for {
		if cStrs[i] == nil {
			return ret
		}
		ret = append(ret, C.GoString(cStrs[i]))
		i++
	}
}
