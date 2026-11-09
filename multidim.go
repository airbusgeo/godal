// Copyright 2026 Airbus Defence and Space
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
#include "godal.h"
#include <stdlib.h>
#include <cpl_string.h>
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// This file wraps GDAL's multidimensional raster API (RFC 75), which exposes
// datasets such as netCDF, HDF5, Zarr or GRIB as a hierarchy of groups
// containing n-dimensional arrays, dimensions and attributes.
//
// Typical read usage:
//
//	ds, _ := godal.Open("forecast.zarr", godal.MultiDimRaster())
//	defer ds.Close()
//	root, _ := ds.RootGroup()
//	defer root.Close()
//	arr, _ := root.OpenMDArray("temperature")
//	defer arr.Close()
//	dims, _ := arr.Dimensions() // e.g. time, lat, lon
//	buf := make([]float32, 1*dims[1].Size*dims[2].Size)
//	_ = arr.Read([]int{0, 0, 0}, []int{1, int(dims[1].Size), int(dims[2].Size)}, buf)

type multiDimRasterOpt struct{}

// MultiDimRaster makes Open() use GDAL's multidimensional raster API
// (GDAL_OF_MULTIDIM_RASTER), so that the returned Dataset exposes its
// content through RootGroup().
func MultiDimRaster() interface {
	OpenOption
} {
	return multiDimRasterOpt{}
}

func (multiDimRasterOpt) setOpenOpt(oo *openOpts) error {
	oo.flags |= C.GDAL_OF_MULTIDIM_RASTER
	return nil
}

// MultiDimOption is an option that can be passed to the methods of Group and MDArray.
//
// Available MultiDimOptions are:
//   - ConfigOption
//   - ErrLogger
type MultiDimOption interface {
	setMultiDimOpt(o *multiDimOpts)
}

type multiDimOpts struct {
	config       []string
	errorHandler ErrorHandler
}

func (ec errorCallback) setMultiDimOpt(o *multiDimOpts) {
	o.errorHandler = ec.fn
}

func (co configOpt) setMultiDimOpt(o *multiDimOpts) {
	o.config = append(o.config, co.config...)
}

func multiDimContext(opts []MultiDimOption) cgoContext {
	mo := multiDimOpts{}
	for _, o := range opts {
		o.setMultiDimOpt(&mo)
	}
	return createCGOContext(mo.config, mo.errorHandler)
}

// MDArrayIOOption is an option that can be passed to MDArray.Read() and MDArray.Write().
//
// Available MDArrayIOOptions are:
//   - Steps
//   - ConfigOption
//   - ErrLogger
type MDArrayIOOption interface {
	setMDArrayIOOpt(o *mdArrayIOOpts)
}

type mdArrayIOOpts struct {
	multiDimOpts
	steps []int
}

func (ec errorCallback) setMDArrayIOOpt(o *mdArrayIOOpts) {
	o.errorHandler = ec.fn
}

func (co configOpt) setMDArrayIOOpt(o *mdArrayIOOpts) {
	o.config = append(o.config, co.config...)
}

type stepsOpt struct {
	steps []int
}

// Steps sets the increment, along each dimension, between two consecutive
// elements read from or written to the array. A step of 2 reads every other
// element, a negative step walks the dimension backwards. Defaults to 1 for
// every dimension.
func Steps(steps ...int) interface {
	MDArrayIOOption
} {
	return stepsOpt{steps}
}

func (so stepsOpt) setMDArrayIOOpt(o *mdArrayIOOpts) {
	o.steps = so.steps
}

// Dimension describes one dimension of a Group or MDArray
type Dimension struct {
	// Name of the dimension, e.g. "time" or "lat"
	Name string
	// Type of the dimension, e.g. "HORIZONTAL_X", "HORIZONTAL_Y", "TEMPORAL", or "" if unknown
	Type string
	// Direction of the dimension, e.g. "EAST", "NORTH", "FUTURE", or "" if unknown
	Direction string
	// Size is the number of elements along this dimension
	Size uint64
	// IndexingVariable is the full name of the array holding the coordinate values of this
	// dimension (e.g. "/lat"), or "" if there is none
	IndexingVariable string
}

// Attribute is a (name, value) pair attached to a Group or an MDArray
type Attribute struct {
	Name string
	// DataType of numeric attributes. Unknown for string (and compound) attributes
	DataType DataType
	// Shape of the attribute value. Empty for scalar attributes
	Shape []uint64
	// Strings holds the value(s) of a string attribute
	Strings []string
	// Values holds the value(s) of a numeric attribute, converted to float64
	Values []float64
}

// IsString returns true if the attribute holds string values
func (a Attribute) IsString() bool {
	return a.Strings != nil
}

// String returns the attribute value formatted as a string. Multiple
// values are separated by commas.
func (a Attribute) String() string {
	if a.IsString() {
		if len(a.Strings) == 1 {
			return a.Strings[0]
		}
		return fmt.Sprint(a.Strings)
	}
	if len(a.Values) == 1 {
		return fmt.Sprint(a.Values[0])
	}
	return fmt.Sprint(a.Values)
}

// Group is a wrapper around a GDALGroupH. Groups are obtained with Dataset.RootGroup(),
// Group.OpenGroup() or Group.CreateGroup() and must be released with Close().
type Group struct {
	handle C.GDALGroupH
}

// MDArray is a wrapper around a GDALMDArrayH, i.e. an n-dimensional array. MDArrays are
// obtained with Group.OpenMDArray(), Group.CreateMDArray() or MDArray.View() and must be
// released with Close().
type MDArray struct {
	handle C.GDALMDArrayH
}

// CreateMultiDimensional creates a new multidimensional dataset with the given driver
// (e.g. "MEM", "netCDF", "Zarr"). Content is added through the returned dataset's
// RootGroup().
func CreateMultiDimensional(driver DriverName, name string, opts ...DatasetCreateOption) (*Dataset, error) {
	drvname := string(driver)
	if drv, ok := driverMappings[driver]; ok {
		if drv.rasterName == "" {
			return nil, fmt.Errorf("%s does not support raster creation", driver)
		}
		drvname = drv.rasterName
	}
	drv, ok := getDriver(drvname)
	if !ok {
		return nil, fmt.Errorf("failed to get driver %s", drvname)
	}
	gopts := dsCreateOpts{}
	for _, opt := range opts {
		opt.setDatasetCreateOpt(&gopts)
	}
	createOpts := sliceToCStringArray(gopts.creation)
	cname := C.CString(name)
	defer createOpts.free()
	defer C.free(unsafe.Pointer(cname))

	cgc := createCGOContext(gopts.config, gopts.errorHandler)
	hndl := C.godalCreateMultiDimensional(cgc.cPointer(), drv.handle(), cname, createOpts.cPointer())
	if err := cgc.close(); err != nil {
		if hndl != nil {
			C.godalClose(nil, hndl)
		}
		return nil, err
	}
	if hndl == nil {
		return nil, fmt.Errorf("failed to create multidimensional dataset %s", name)
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// RootGroup returns the root group of a multidimensional dataset, i.e. one opened
// with the MultiDimRaster() option or created with CreateMultiDimensional().
// The returned Group must be closed after use.
func (ds *Dataset) RootGroup(opts ...MultiDimOption) (*Group, error) {
	cgc := multiDimContext(opts)
	hndl := C.godalDatasetGetRootGroup(cgc.cPointer(), ds.handle())
	if err := cgc.close(); err != nil {
		if hndl != nil {
			C.godalGroupRelease(hndl)
		}
		return nil, err
	}
	if hndl == nil {
		return nil, fmt.Errorf("dataset has no multidimensional root group")
	}
	return &Group{handle: hndl}, nil
}

func takeCString(cstr *C.char) string {
	defer C.VSIFree(unsafe.Pointer(cstr))
	return C.GoString(cstr)
}

func takeCStringList(cstrs **C.char) []string {
	defer C.CSLDestroy(cstrs)
	ret := cStringArrayToSlice(cstrs)
	if ret == nil {
		return []string{}
	}
	return ret
}

// Close releases the group
func (g *Group) Close() {
	if g.handle == nil {
		return
	}
	C.godalGroupRelease(g.handle)
	g.handle = nil
}

// Name returns the group name ("/" for the root group)
func (g *Group) Name() string {
	return takeCString(C.godalGroupGetName(g.handle))
}

// FullName returns the full path of the group, e.g. "/forecast/surface"
func (g *Group) FullName() string {
	return takeCString(C.godalGroupGetFullName(g.handle))
}

// GroupNames returns the names of the direct subgroups of g
func (g *Group) GroupNames(opts ...MultiDimOption) ([]string, error) {
	cgc := multiDimContext(opts)
	cnames := C.godalGroupGetGroupNames(cgc.cPointer(), g.handle)
	names := takeCStringList(cnames)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return names, nil
}

// MDArrayNames returns the names of the arrays contained in g
func (g *Group) MDArrayNames(opts ...MultiDimOption) ([]string, error) {
	cgc := multiDimContext(opts)
	cnames := C.godalGroupGetMDArrayNames(cgc.cPointer(), g.handle)
	names := takeCStringList(cnames)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return names, nil
}

// OpenGroup opens the subgroup with the given name. The returned Group must be closed after use.
func (g *Group) OpenGroup(name string, opts ...MultiDimOption) (*Group, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	cgc := multiDimContext(opts)
	hndl := C.godalGroupOpenGroup(cgc.cPointer(), g.handle, cname)
	if err := cgc.close(); err != nil {
		if hndl != nil {
			C.godalGroupRelease(hndl)
		}
		return nil, err
	}
	if hndl == nil {
		return nil, fmt.Errorf("group %s not found", name)
	}
	return &Group{handle: hndl}, nil
}

// OpenMDArray opens the array with the given name. The returned MDArray must be closed after use.
func (g *Group) OpenMDArray(name string, opts ...MultiDimOption) (*MDArray, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	cgc := multiDimContext(opts)
	hndl := C.godalGroupOpenMDArray(cgc.cPointer(), g.handle, cname)
	if err := cgc.close(); err != nil {
		if hndl != nil {
			C.godalMDArrayRelease(hndl)
		}
		return nil, err
	}
	if hndl == nil {
		return nil, fmt.Errorf("array %s not found", name)
	}
	return &MDArray{handle: hndl}, nil
}

// CreateGroup creates a subgroup. The returned Group must be closed after use.
func (g *Group) CreateGroup(name string, opts ...MultiDimOption) (*Group, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	cgc := multiDimContext(opts)
	hndl := C.godalGroupCreateGroup(cgc.cPointer(), g.handle, cname)
	if err := cgc.close(); err != nil {
		if hndl != nil {
			C.godalGroupRelease(hndl)
		}
		return nil, err
	}
	if hndl == nil {
		return nil, fmt.Errorf("failed to create group %s", name)
	}
	return &Group{handle: hndl}, nil
}

// CreateDimension creates a dimension in the group. Only dim.Name, dim.Type, dim.Direction
// and dim.Size are used.
func (g *Group) CreateDimension(dim Dimension, opts ...MultiDimOption) error {
	cname := C.CString(dim.Name)
	ctype := C.CString(dim.Type)
	cdir := C.CString(dim.Direction)
	defer C.free(unsafe.Pointer(cname))
	defer C.free(unsafe.Pointer(ctype))
	defer C.free(unsafe.Pointer(cdir))
	cgc := multiDimContext(opts)
	C.godalGroupCreateDimension(cgc.cPointer(), g.handle, cname, ctype, cdir, C.GUInt64(dim.Size))
	return cgc.close()
}

// CreateMDArray creates an array of the given numeric data type, whose dimensions are the
// group's dimensions with the given names (slowest varying first, e.g. "time", "lat", "lon").
// The returned MDArray must be closed after use.
func (g *Group) CreateMDArray(name string, dimensions []string, dtype DataType, opts ...MultiDimOption) (*MDArray, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	cdims := sliceToCStringArray(dimensions)
	defer cdims.free()
	cgc := multiDimContext(opts)
	hndl := C.godalGroupCreateMDArray(cgc.cPointer(), g.handle, cname, cdims.cPointer(), C.GDALDataType(dtype), nil)
	if err := cgc.close(); err != nil {
		if hndl != nil {
			C.godalMDArrayRelease(hndl)
		}
		return nil, err
	}
	if hndl == nil {
		return nil, fmt.Errorf("failed to create array %s", name)
	}
	return &MDArray{handle: hndl}, nil
}

// Dimensions returns the dimensions declared in the group
func (g *Group) Dimensions(opts ...MultiDimOption) ([]Dimension, error) {
	cgc := multiDimContext(opts)
	var count C.size_t
	cdims := C.godalGroupGetDimensions(cgc.cPointer(), g.handle, &count)
	dims := goDimensions(cdims, count)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return dims, nil
}

// Attributes returns the attributes attached to the group
func (g *Group) Attributes(opts ...MultiDimOption) ([]Attribute, error) {
	cgc := multiDimContext(opts)
	var count C.size_t
	cattrs := C.godalGroupGetAttributes(cgc.cPointer(), g.handle, &count)
	attrs := goAttributes(cattrs, count)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return attrs, nil
}

// SetAttribute creates an attribute on the group. value must be a string, a float64
// or a []float64.
func (g *Group) SetAttribute(name string, value interface{}, opts ...MultiDimOption) error {
	return setAttribute(name, value, opts, func(cgc cgoContext, cname, cstr *C.char, cvals *C.double, n C.size_t) {
		C.godalGroupSetAttribute(cgc.cPointer(), g.handle, cname, cstr, cvals, n)
	})
}

// Close releases the array
func (a *MDArray) Close() {
	if a.handle == nil {
		return
	}
	C.godalMDArrayRelease(a.handle)
	a.handle = nil
}

// Name returns the array name
func (a *MDArray) Name() string {
	return takeCString(C.godalMDArrayGetName(a.handle))
}

// FullName returns the full path of the array, e.g. "/forecast/temperature"
func (a *MDArray) FullName() string {
	return takeCString(C.godalMDArrayGetFullName(a.handle))
}

// DataType returns the data type of the array elements. It returns Unknown
// for non numeric (i.e. string or compound) arrays.
func (a *MDArray) DataType() DataType {
	var isNumeric C.int
	return DataType(C.godalMDArrayGetDataType(a.handle, &isNumeric))
}

// Dimensions returns the dimensions of the array, slowest varying first
func (a *MDArray) Dimensions(opts ...MultiDimOption) ([]Dimension, error) {
	cgc := multiDimContext(opts)
	var count C.size_t
	cdims := C.godalMDArrayGetDimensions(cgc.cPointer(), a.handle, &count)
	dims := goDimensions(cdims, count)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return dims, nil
}

// Shape returns the size of each dimension of the array, slowest varying first
func (a *MDArray) Shape(opts ...MultiDimOption) ([]uint64, error) {
	dims, err := a.Dimensions(opts...)
	if err != nil {
		return nil, err
	}
	shape := make([]uint64, len(dims))
	for i, d := range dims {
		shape[i] = d.Size
	}
	return shape, nil
}

// Attributes returns the attributes attached to the array
func (a *MDArray) Attributes(opts ...MultiDimOption) ([]Attribute, error) {
	cgc := multiDimContext(opts)
	var count C.size_t
	cattrs := C.godalMDArrayGetAttributes(cgc.cPointer(), a.handle, &count)
	attrs := goAttributes(cattrs, count)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return attrs, nil
}

// SetAttribute creates an attribute on the array. value must be a string, a float64
// or a []float64.
func (a *MDArray) SetAttribute(name string, value interface{}, opts ...MultiDimOption) error {
	return setAttribute(name, value, opts, func(cgc cgoContext, cname, cstr *C.char, cvals *C.double, n C.size_t) {
		C.godalMDArraySetAttribute(cgc.cPointer(), a.handle, cname, cstr, cvals, n)
	})
}

// NoData returns the array's nodata value, and a boolean indicating if a nodata value is set
func (a *MDArray) NoData() (nodata float64, ok bool) {
	var cok C.int
	cn := C.godalMDArrayGetNoData(a.handle, &cok)
	return float64(cn), cok != 0
}

// SetNoData sets the array's nodata value
func (a *MDArray) SetNoData(nd float64, opts ...MultiDimOption) error {
	cgc := multiDimContext(opts)
	C.godalMDArraySetNoData(cgc.cPointer(), a.handle, C.double(nd))
	return cgc.close()
}

// ScaleOffset returns the scale and offset to apply to raw values (unpacked = raw*scale + offset).
// Defaults to 1 and 0 when unset.
func (a *MDArray) ScaleOffset() (scale, offset float64) {
	var hasScale, hasOffset C.int
	scale = float64(C.godalMDArrayGetScale(a.handle, &hasScale))
	offset = float64(C.godalMDArrayGetOffset(a.handle, &hasOffset))
	if hasScale == 0 {
		scale = 1
	}
	if hasOffset == 0 {
		offset = 0
	}
	return
}

// SetScaleOffset sets the scale and offset to apply to raw values
func (a *MDArray) SetScaleOffset(scale, offset float64, opts ...MultiDimOption) error {
	cgc := multiDimContext(opts)
	C.godalMDArraySetScaleOffset(cgc.cPointer(), a.handle, C.double(scale), C.double(offset))
	return cgc.close()
}

// Unit returns the array's unit (e.g. "K" or "m/s"), or "" if unset
func (a *MDArray) Unit() string {
	return takeCString(C.godalMDArrayGetUnit(a.handle))
}

// SetUnit sets the array's unit
func (a *MDArray) SetUnit(unit string, opts ...MultiDimOption) error {
	cunit := C.CString(unit)
	defer C.free(unsafe.Pointer(cunit))
	cgc := multiDimContext(opts)
	C.godalMDArraySetUnit(cgc.cPointer(), a.handle, cunit)
	return cgc.close()
}

// SpatialRef returns the array's spatial reference, or nil if the array has none.
// The returned SpatialRef should be closed after use.
func (a *MDArray) SpatialRef() *SpatialRef {
	hndl := C.godalMDArrayGetSpatialRef(a.handle)
	if hndl == nil {
		return nil
	}
	return &SpatialRef{handle: hndl, isOwned: true}
}

// Read reads the hyper-rectangle of the array starting at index start and spanning
// count elements along each dimension into buffer, in row-major order (last dimension
// varying fastest). buffer must be a slice of a numeric type supported by godal
// (e.g. []float32, []int16) and is converted from the array's data type if needed.
// With the Steps() option, count is the number of elements read along each dimension.
func (a *MDArray) Read(start, count []int, buffer interface{}, opts ...MDArrayIOOption) error {
	return a.io(false, start, count, buffer, opts)
}

// Write writes buffer to the hyper-rectangle of the array starting at index start and
// spanning count elements along each dimension. See Read for the buffer layout.
func (a *MDArray) Write(start, count []int, buffer interface{}, opts ...MDArrayIOOption) error {
	return a.io(true, start, count, buffer, opts)
}

func (a *MDArray) io(write bool, start, count []int, buffer interface{}, opts []MDArrayIOOption) error {
	iopts := mdArrayIOOpts{}
	for _, o := range opts {
		o.setMDArrayIOOpt(&iopts)
	}
	if len(start) != len(count) {
		return fmt.Errorf("start and count must have the same length (got %d and %d)", len(start), len(count))
	}
	if iopts.steps != nil && len(iopts.steps) != len(start) {
		return fmt.Errorf("steps must have one value per dimension (got %d, expected %d)", len(iopts.steps), len(start))
	}
	ndims := len(start)
	if ndims == 0 {
		// scalar arrays: a single element
		start = []int{0}
		count = []int{1}
	}
	total := 1
	cstart := make([]C.GUInt64, len(start))
	ccount := make([]C.size_t, len(count))
	for i := range start {
		if start[i] < 0 || count[i] < 0 {
			return fmt.Errorf("start and count must not be negative")
		}
		cstart[i] = C.GUInt64(start[i])
		ccount[i] = C.size_t(count[i])
		total *= count[i]
	}
	if total == 0 {
		return nil
	}
	dtype := bufferType(buffer)
	cbuf := cBuffer(buffer, total)
	var cstep *C.GInt64
	if iopts.steps != nil {
		csteps := make([]C.GInt64, len(iopts.steps))
		for i, s := range iopts.steps {
			csteps[i] = C.GInt64(s)
		}
		cstep = &csteps[0]
	}

	cw := C.int(0)
	if write {
		cw = 1
	}
	cgc := createCGOContext(iopts.config, iopts.errorHandler)
	C.godalMDArrayIO(cgc.cPointer(), a.handle, cw, &cstart[0], &ccount[0], cstep,
		C.GDALDataType(dtype), cbuf, C.size_t(total*dtype.Size()))
	return cgc.close()
}

// View returns a view of the array described by a slicing expression, using the
// same syntax as GDAL's GetView(), e.g. "[0,...]" to select the first time step of a
// (time, lat, lon) array, "[:,::-1,:]" to flip the second dimension, or "['field']" to
// extract a field of a compound array. The returned MDArray must be closed after use.
func (a *MDArray) View(expr string, opts ...MultiDimOption) (*MDArray, error) {
	cexpr := C.CString(expr)
	defer C.free(unsafe.Pointer(cexpr))
	cgc := multiDimContext(opts)
	hndl := C.godalMDArrayGetView(cgc.cPointer(), a.handle, cexpr)
	if err := cgc.close(); err != nil {
		if hndl != nil {
			C.godalMDArrayRelease(hndl)
		}
		return nil, err
	}
	if hndl == nil {
		return nil, fmt.Errorf("failed to create view %s", expr)
	}
	return &MDArray{handle: hndl}, nil
}

// AsClassicDataset exposes the array as a classic (2D) raster Dataset, using the
// dimensions at index xdim and ydim as the raster's x and y axes (other dimensions,
// if any, become bands). This allows using e.g. Warp() or Translate() on a slice of
// a multidimensional array. The array must stay open while the returned Dataset is used,
// and the Dataset must be closed after use.
func (a *MDArray) AsClassicDataset(xdim, ydim int, opts ...MultiDimOption) (*Dataset, error) {
	if xdim < 0 || ydim < 0 {
		return nil, fmt.Errorf("dimension indexes must not be negative")
	}
	cgc := multiDimContext(opts)
	hndl := C.godalMDArrayAsClassicDataset(cgc.cPointer(), a.handle, C.size_t(xdim), C.size_t(ydim))
	if err := cgc.close(); err != nil {
		if hndl != nil {
			C.godalClose(nil, hndl)
		}
		return nil, err
	}
	if hndl == nil {
		return nil, fmt.Errorf("failed to expose array as a classic dataset")
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

func goDimensions(cdims *C.godalDimension, count C.size_t) []Dimension {
	if cdims == nil {
		return nil
	}
	defer C.godalFreeDimensions(cdims, count)
	sl := unsafe.Slice(cdims, int(count))
	dims := make([]Dimension, len(sl))
	for i, cd := range sl {
		dims[i] = Dimension{
			Name:             C.GoString(cd.name),
			Type:             C.GoString(cd._type),
			Direction:        C.GoString(cd.direction),
			Size:             uint64(cd.size),
			IndexingVariable: C.GoString(cd.indexingVariable),
		}
	}
	return dims
}

func goAttributes(cattrs *C.godalAttribute, count C.size_t) []Attribute {
	if cattrs == nil {
		return nil
	}
	defer C.godalFreeAttributes(cattrs, count)
	sl := unsafe.Slice(cattrs, int(count))
	attrs := make([]Attribute, len(sl))
	for i, ca := range sl {
		attr := Attribute{
			Name:     C.GoString(ca.name),
			DataType: DataType(ca.dataType),
		}
		if ca.nDims > 0 {
			attr.Shape = make([]uint64, int(ca.nDims))
			for d, v := range unsafe.Slice(ca.dims, int(ca.nDims)) {
				attr.Shape[d] = uint64(v)
			}
		}
		if ca.isString != 0 {
			attr.Strings = []string{}
			if ca.nValues > 0 {
				for _, s := range unsafe.Slice(ca.strings, int(ca.nValues)) {
					attr.Strings = append(attr.Strings, C.GoString(s))
				}
			}
		} else if ca.nValues > 0 {
			attr.Values = make([]float64, int(ca.nValues))
			for v, val := range unsafe.Slice(ca.values, int(ca.nValues)) {
				attr.Values[v] = float64(val)
			}
		}
		attrs[i] = attr
	}
	return attrs
}

func setAttribute(name string, value interface{}, opts []MultiDimOption,
	fn func(cgc cgoContext, cname, cstr *C.char, cvals *C.double, n C.size_t)) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	var cstr *C.char
	var values []float64
	switch v := value.(type) {
	case string:
		cstr = C.CString(v)
		defer C.free(unsafe.Pointer(cstr))
	case float64:
		values = []float64{v}
	case []float64:
		if len(v) == 0 {
			return fmt.Errorf("attribute %s: empty value", name)
		}
		values = v
	default:
		return fmt.Errorf("attribute %s: unsupported value type %T (expected string, float64 or []float64)", name, value)
	}
	var cvals *C.double
	if values != nil {
		cvals = (*C.double)(unsafe.Pointer(&values[0]))
	}
	cgc := multiDimContext(opts)
	fn(cgc, cname, cstr, cvals, C.size_t(len(values)))
	return cgc.close()
}
