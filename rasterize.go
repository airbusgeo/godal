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
	"unsafe"
)

type rasterizeOpts struct {
	create []string
	config []string
	driver DriverName
}

// RasterizeOption is an option that can be passed to Rasterize()
//
// Available RasterizeOptions are:
//
// • CreationOption
//
// • ConfigOption
//
// • DriverName
type RasterizeOption interface {
	setRasterizeOpt(ro *rasterizeOpts)
}

// Rasterize wraps GDALRasterize()
func (ds *Dataset) Rasterize(dstDS string, switches []string, opts ...RasterizeOption) (*Dataset, error) {
	gopts := rasterizeOpts{}
	for _, opt := range opts {
		opt.setRasterizeOpt(&gopts)
	}
	for _, copt := range gopts.create {
		switches = append(switches, "-co", copt)
	}
	if gopts.driver != "" {
		dname := string(gopts.driver)
		if dm, ok := driverMappings[gopts.driver]; ok {
			dname = dm.rasterName
		}
		switches = append(switches, "-of", dname)
	}
	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()
	cconfig := sliceToCStringArray(gopts.config)
	defer cconfig.free()

	cname := unsafe.Pointer(C.CString(dstDS))
	defer C.free(cname)

	var errmsg *C.char
	hndl := C.godalRasterize((*C.char)(cname), ds.Handle(), cswitches.cPointer(), &errmsg, cconfig.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

type rasterizeGeometryOpt struct {
	bands      []int
	values     []float64
	allTouched C.int
}

// RasterizeGeometryOption is an option that can be passed tp Dataset.RasterizeGeometry()
type RasterizeGeometryOption interface {
	setRasterizeGeometryOpt(o *rasterizeGeometryOpt)
}

type allTouchedOpt struct{}

func (at allTouchedOpt) setRasterizeGeometryOpt(o *rasterizeGeometryOpt) {
	o.allTouched = C.int(1)
}

// AllTouched is an option that can be passed to Dataset.RasterizeGeometries()
// where all pixels touched by lines or polygons will be updated, not just those on the line
// render path, or whose center point is within the polygon.
func AllTouched() interface {
	RasterizeGeometryOption
} {
	return allTouchedOpt{}
}

// RasterizeGeometry "burns" the provided geometry onto ds.
// By default, the "0" value is burned into all of ds's bands. This behavior can be modified
// with the following options:
//
// • Bands(bnd ...int) the list of bands to affect
//
// • Values(val ...float64) the pixel value to burn. There must be either 1 or len(bands) values
// provided
//
// • AllTouched() pixels touched by lines or polygons will be updated, not just those on the line
// render path, or whose center point is within the polygon.
//
func (ds *Dataset) RasterizeGeometry(g *Geometry, opts ...RasterizeGeometryOption) error {
	opt := rasterizeGeometryOpt{}
	for _, o := range opts {
		o.setRasterizeGeometryOpt(&opt)
	}
	if len(opt.bands) == 0 {
		bnds := ds.Bands()
		opt.bands = make([]int, len(bnds))
		for i := range bnds {
			opt.bands[i] = i + 1
		}
	}
	if len(opt.values) == 0 {
		opt.values = make([]float64, len(opt.bands))
		for i := range opt.values {
			opt.values[i] = 0
		}
	}
	if len(opt.values) == 1 && len(opt.values) != len(opt.bands) {
		for i := 1; i < len(opt.bands); i++ {
			opt.values = append(opt.values, opt.values[0])
		}
	}
	if len(opt.values) != len(opt.bands) {
		return fmt.Errorf("must pass in same number of values as bands")
	}
	errmsg := C.godalRasterizeGeometry(ds.Handle(), g.handle,
		cIntArray(opt.bands), C.int(len(opt.bands)), cDoubleArray(opt.values), opt.allTouched)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil

}
