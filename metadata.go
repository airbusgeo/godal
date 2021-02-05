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
	"strings"
	"unsafe"
)

type metadataOpt struct {
	domain string
}

// MetadataOption is an option that can be passed to metadata related calls
// Available MetadataOptions are:
//
// • Domain
type MetadataOption interface {
	setMetadataOpt(mo *metadataOpt)
}

// Domain specifies the gdal metadata domain to use
func Domain(mdDomain string) interface {
	MetadataOption
} {
	return metadataOpt{mdDomain}
}
func (mdo metadataOpt) setMetadataOpt(mo *metadataOpt) {
	mo.domain = mdo.domain
}

func (mo majorObject) Metadata(key string, opts ...MetadataOption) string {
	mopts := metadataOpt{}
	for _, opt := range opts {
		opt.setMetadataOpt(&mopts)
	}
	ckey := C.CString(key)
	cdom := C.CString(mopts.domain)
	defer C.free(unsafe.Pointer(ckey))
	defer C.free(unsafe.Pointer(cdom))
	str := C.GDALGetMetadataItem(mo.handle, ckey, cdom)
	return C.GoString(str)
}

func (mo majorObject) Metadatas(opts ...MetadataOption) map[string]string {
	mopts := metadataOpt{}
	for _, opt := range opts {
		opt.setMetadataOpt(&mopts)
	}
	cdom := C.CString(mopts.domain)
	defer C.free(unsafe.Pointer(cdom))
	strs := C.GDALGetMetadata(mo.handle, cdom)
	strslice := cStringArrayToSlice(strs)
	if len(strslice) == 0 {
		return nil
	}
	ret := make(map[string]string)
	for _, str := range strslice {
		idx := strings.Index(str, "=")
		if idx == -1 || idx == len(str)-1 {
			ret[str[0:len(str)-1]] = ""
		} else {
			ret[str[0:idx]] = str[idx+1:]
		}
	}
	return ret
}

func (mo majorObject) SetMetadata(key, value string, opts ...MetadataOption) error {
	mopts := metadataOpt{}
	for _, opt := range opts {
		opt.setMetadataOpt(&mopts)
	}
	ckey := C.CString(key)
	cval := C.CString(value)
	cdom := C.CString(mopts.domain)
	defer C.free(unsafe.Pointer(ckey))
	defer C.free(unsafe.Pointer(cdom))
	defer C.free(unsafe.Pointer(cval))
	errmsg := C.godalSetMetadataItem(mo.handle, ckey, cval, cdom)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

func (mo majorObject) MetadataDomains() []string {
	strs := C.GDALGetMetadataDomainList(mo.handle)
	return cStringArrayToSlice(strs)
}
