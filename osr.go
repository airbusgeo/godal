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

//SpatialRef is a wrapper around OGRSpatialReferenceH
type SpatialRef struct {
	handle  C.OGRSpatialReferenceH
	isOwned bool
}

type srWKTOpts struct{}

//WKTExportOption is an option that can be passed to SpatialRef.WKT()
//
// Available WKTExportOptions are:
//
// • TODO
type WKTExportOption interface {
	setWKTExportOpts(sro *srWKTOpts)
}

//WKT returns spatialrefernece as WKT
func (sr *SpatialRef) WKT(opts ...WKTExportOption) (string, error) {
	var errmsg *C.char
	cwkt := C.godalExportToWKT(sr.handle, &errmsg)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return "", errors.New(C.GoString(errmsg))
	}
	wkt := C.GoString(cwkt)
	C.CPLFree(unsafe.Pointer(cwkt))
	return wkt, nil
}

//Close releases memory
func (sr *SpatialRef) Close() {
	if sr.handle == nil {
		return
		//panic("handle already closed")
	}
	if !sr.isOwned {
		sr.handle = nil
		return
	}
	C.OSRRelease(sr.handle)
	sr.handle = nil
}

// NewSpatialRefFromWKT creates a SpatialRef from an opengis WKT description
func NewSpatialRefFromWKT(wkt string) (*SpatialRef, error) {
	cstr := C.CString(wkt)
	defer C.free(unsafe.Pointer(cstr))
	var errmsg *C.char
	hndl := C.godalCreateWKTSpatialRef((*C.char)(unsafe.Pointer(cstr)), &errmsg)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &SpatialRef{handle: hndl, isOwned: true}, nil
}

// NewSpatialRefFromProj4 creates a SpatialRef from a proj4 string
func NewSpatialRefFromProj4(proj string) (*SpatialRef, error) {
	cstr := C.CString(proj)
	defer C.free(unsafe.Pointer(cstr))
	var errmsg *C.char
	hndl := C.godalCreateProj4SpatialRef((*C.char)(unsafe.Pointer(cstr)), &errmsg)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &SpatialRef{handle: hndl, isOwned: true}, nil
}

// NewSpatialRefFromEPSG creates a SpatialRef from an epsg code
func NewSpatialRefFromEPSG(code int) (*SpatialRef, error) {
	var errmsg *C.char
	hndl := C.godalCreateEPSGSpatialRef(C.int(code), &errmsg)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &SpatialRef{handle: hndl, isOwned: true}, nil
}

// IsSame returns whether two SpatiaRefs describe the same projection.
func (sr *SpatialRef) IsSame(other *SpatialRef) bool {
	ret := C.OSRIsSame(sr.handle, other.handle)
	return ret != 0
}

// Transform transforms coordinates from one SpatialRef to another
type Transform struct {
	handle C.OGRCoordinateTransformationH
	dst    C.OGRSpatialReferenceH //TODO: refcounting/freeing on this?
}

type trnOpts struct{}

// TransformOption is an option that can be passed to NewTransform
//
// Available TransformOptions are:
//
// • TODO
type TransformOption interface {
	setTransformOpt(o *trnOpts)
}

// NewTransform creates a transformation object from src to dst
func NewTransform(src, dst *SpatialRef, opts ...TransformOption) (*Transform, error) {
	var errmsg *C.char
	hndl := C.godalNewCoordinateTransformation(src.handle, dst.handle, &errmsg)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Transform{handle: hndl, dst: dst.handle}, nil
}

// Close releases the Transform object
func (trn *Transform) Close() {
	if trn.handle == nil {
		return
		//panic("transform already closed")
	}
	C.OCTDestroyCoordinateTransformation(trn.handle)
	trn.handle = nil
}

// TransformEx reprojects points in place
//
// x and y may not be nil and must be of the same length
//
// z may be nil, or of the same length as x and y
//
// successful may be nil or of the same length as x and y. If non nil, it will contain
// true or false depending on wether the corresponding point succeeded transformation or not.
//
// TODO: create a version of this function that accepts *C.double to avoid allocs?
// TODO: create a Transform() method that accepts z and successful as options
func (trn *Transform) TransformEx(x []float64, y []float64, z []float64, successful []bool) error {
	cx := make([]C.double, len(x))
	cy := make([]C.double, len(x))
	pcx, pcy := (*C.double)(unsafe.Pointer(&cx[0])), (*C.double)(unsafe.Pointer(&cy[0]))
	pcz := (*C.double)(nil)
	pcs := (*C.int)(nil)
	var cz []C.double
	var cs []C.int
	if len(z) > 0 {
		cz = make([]C.double, len(x))
		pcz = (*C.double)(unsafe.Pointer(&cz[0]))
	}
	if len(successful) > 0 {
		cs = make([]C.int, len(x))
		pcs = (*C.int)(unsafe.Pointer(&cs[0]))
	}
	for i := range x {
		cx[i] = C.double(x[i])
		cy[i] = C.double(y[i])
		if cz != nil {
			cz[i] = C.double(z[i])
		}
	}
	ret := C.OCTTransformEx(trn.handle, C.int(len(x)), pcx, pcy, pcz, pcs)
	for i := range x {
		x[i] = float64(cx[i])
		y[i] = float64(cy[i])
		if cz != nil {
			z[i] = float64(cz[i])
		}
		if cs != nil {
			if cs[i] > 0 {
				successful[i] = true
			} else {
				successful[i] = false
			}
		}
	}
	if ret == 0 {
		return fmt.Errorf("some or all points failed to transform")
	}
	return nil
}

// Geographic returns wether the SpatialRef is geographic
func (sr *SpatialRef) Geographic() bool {
	ret := C.OSRIsGeographic(sr.handle)
	return ret != 0
}

// SemiMajor returns the SpatialRef's Semi Major Axis
func (sr *SpatialRef) SemiMajor() (float64, error) {
	var err C.int
	sm := C.OSRGetSemiMajor(sr.handle, &err)
	if err != 0 {
		return float64(sm), fmt.Errorf("ogr error %d", err)
	}
	return float64(sm), nil
}

// SemiMinor returns the SpatialRef's Semi Minor Axis
func (sr *SpatialRef) SemiMinor() (float64, error) {
	var err C.int
	sm := C.OSRGetSemiMinor(sr.handle, &err)
	if err != 0 {
		return float64(sm), fmt.Errorf("ogr error %d", err)
	}
	return float64(sm), nil
}

// AuthorityName is used to query an AUTHORITY[] node from within the WKT tree, and fetch the authority name value.
//
// target is the partial or complete path to the node to get an authority from. i.e. "PROJCS", "GEOGCS", "GEOGCS|UNIT"
// or "" to search for an authority node on the root element.
func (sr *SpatialRef) AuthorityName(target string) string {
	cstr := (*C.char)(nil)
	if len(target) > 0 {
		cstr = C.CString(target)
		defer C.free(unsafe.Pointer(cstr))
	}
	cret := C.OSRGetAuthorityName(sr.handle, cstr)
	if cret != nil {
		return C.GoString(cret)
	}
	return ""
}

// AuthorityCode is used to query an AUTHORITY[] node from within the WKT tree, and fetch the code value.
// target is the partial or complete path to the node to get an authority from. i.e. "PROJCS", "GEOGCS", "GEOGCS|UNIT"
// or "" to search for an authority node on the root element.
//
// While in theory values may be non-numeric, for the EPSG authority all code values should be integral.
func (sr *SpatialRef) AuthorityCode(target string) string {
	cstr := (*C.char)(nil)
	if len(target) > 0 {
		cstr = C.CString(target)
		defer C.free(unsafe.Pointer(cstr))
	}
	cret := C.OSRGetAuthorityCode(sr.handle, cstr)
	if cret != nil {
		return C.GoString(cret)
	}
	return ""
}

// AutoIdentifyEPSG sets EPSG authority info if possible.
func (sr *SpatialRef) AutoIdentifyEPSG() error {
	ogrerr := C.OSRAutoIdentifyEPSG(sr.handle)
	if ogrerr != 0 {
		return fmt.Errorf("ogr error %d", ogrerr)
	}
	return nil
}

type boundsOpt struct {
	sr *SpatialRef
}

// BoundsOption is an option that can be passed to Dataset.Bounds or Geometry.Bounds
//
// Available options are:
//
// • *SpatialRef
type BoundsOption interface {
	setBoundsOpt(o *boundsOpt)
}

func reprojectBounds(bnds [4]float64, src, dst *SpatialRef) ([4]float64, error) {
	var ret [4]float64
	trn, err := NewTransform(src, dst)
	if err != nil {
		return ret, fmt.Errorf("create coordinate transform: %w", err)
	}
	defer trn.Close()
	x := []float64{bnds[0], bnds[0], bnds[2], bnds[2]}
	y := []float64{bnds[1], bnds[3], bnds[3], bnds[1]}
	err = trn.TransformEx(x, y, nil, nil)
	if err != nil {
		return ret, fmt.Errorf("reproject bounds: %w", err)
	}
	ret[0] = x[0]
	ret[1] = y[0]
	ret[2] = x[0]
	ret[3] = y[0]
	for i := 1; i < 4; i++ {
		if x[i] < ret[0] {
			ret[0] = x[i]
		}
		if x[i] > ret[2] {
			ret[2] = x[i]
		}
		if y[i] < ret[1] {
			ret[1] = y[i]
		}
		if y[i] > ret[3] {
			ret[3] = y[i]
		}
	}
	return ret, nil
}
