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

//#include "godal.h"
import "C"
import (
	"errors"
	"fmt"
	"io"
	"unsafe"
)

//VSIFile is a handler around gdal's vsi handlers
type VSIFile struct {
	handle *C.VSILFILE
}

//VSIOpen opens path. path can be virtual, eg beginning with /vsimem/
func VSIOpen(path string) (*VSIFile, error) {
	cname := unsafe.Pointer(C.CString(path))
	defer C.free(cname)
	var errmsg *C.char
	hndl := C.godalVSIOpen((*C.char)(cname), &errmsg)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &VSIFile{hndl}, nil
}

//Close closes the VSIFile. Must be called exactly once.
func (vf *VSIFile) Close() error {
	if vf.handle == nil {
		return fmt.Errorf("already closed")
	}
	errmsg := C.godalVSIClose(vf.handle)
	vf.handle = nil
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

//VSIUnlink deletes path
func VSIUnlink(path string) error {
	cname := unsafe.Pointer(C.CString(path))
	defer C.free(cname)
	errmsg := C.godalVSIUnlink((*C.char)(cname))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

var _ io.ReadCloser = &VSIFile{}

// Read is the standard io.Reader interface
func (vf *VSIFile) Read(buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	var errmsg *C.char
	n := C.godalVSIRead(vf.handle, unsafe.Pointer(&buf[0]), C.int(len(buf)), &errmsg)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return int(n), errors.New(C.GoString(errmsg))
	}
	if int(n) != len(buf) {
		return int(n), io.EOF
	}
	return int(n), nil
}
