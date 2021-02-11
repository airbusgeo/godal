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

// Bands returns all dataset bands.
func (ds *Dataset) Bands() []Band {
	cbands := C.godalRasterBands(ds.Handle())
	if cbands == nil {
		return nil
	}
	defer C.free(unsafe.Pointer(cbands))
	//https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
	sBands := (*[1 << 30]C.GDALRasterBandH)(unsafe.Pointer(cbands))
	bands := []Band{}
	i := 0
	for {
		if sBands[i] == nil {
			return bands
		}
		bands = append(bands, Band{majorObject{C.GDALMajorObjectH(sBands[i])}})
		i++
	}
}

// Bounds returns the dataset's bounding box in the order
//  [MinX, MinY, MaxX, MaxY]
func (ds *Dataset) Bounds(opts ...BoundsOption) ([4]float64, error) {

	bo := boundsOpt{}
	for _, o := range opts {
		o.setBoundsOpt(&bo)
	}
	ret := [4]float64{}
	st := ds.Structure()
	gt, err := ds.GeoTransform()
	if err != nil {
		return ret, fmt.Errorf("get geotransform: %w", err)
	}
	ret[0] = gt[0]
	ret[1] = gt[3]
	ret[2] = gt[0] + float64(st.SizeX)*gt[1] + float64(st.SizeY)*gt[2]
	ret[3] = gt[3] + float64(st.SizeX)*gt[4] + float64(st.SizeY)*gt[5]
	if bo.sr != nil {
		srcsr := ds.SpatialRef()
		defer srcsr.Close()
		ret, err = reprojectBounds(ret, srcsr, bo.sr)
		if err != nil {
			return ret, err
		}
	}
	if ret[0] > ret[2] {
		ret[2], ret[0] = ret[0], ret[2]
	}
	if ret[1] > ret[3] {
		ret[3], ret[1] = ret[1], ret[3]
	}
	return ret, nil
}

type dsCreateMaskOpts struct {
	config []string
}

// DatasetCreateMaskOption is an option that can be passed to Dataset.CreateMaskBand()
//
// Available DatasetCreateMaskOptions are:
//
// • ConfigOption
type DatasetCreateMaskOption interface {
	setDatasetCreateMaskOpt(dcm *dsCreateMaskOpts)
}

//CreateMaskBand creates a mask (nodata) band shared for all bands of this dataset.
//Any handle returned by a previous call to Band.MaskBand() should not be used after a call to CreateMaskBand
//See https://gdal.org/development/rfc/rfc15_nodatabitmask.html for how flag should be used
func (ds *Dataset) CreateMaskBand(flags int, opts ...DatasetCreateMaskOption) (Band, error) {
	gopts := dsCreateMaskOpts{}
	for _, opt := range opts {
		opt.setDatasetCreateMaskOpt(&gopts)
	}
	cconfig := sliceToCStringArray(gopts.config)
	defer cconfig.free()

	var errmsg *C.char
	hndl := C.godalCreateDatasetMaskBand(ds.Handle(), C.int(flags), &errmsg, cconfig.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return Band{}, errors.New(C.GoString(errmsg))
	}
	return Band{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// Projection returns the WKT projection of the dataset. May be empty.
func (ds *Dataset) Projection() string {
	str := C.GDALGetProjectionRef(ds.Handle())
	return C.GoString(str)
}

// SetProjection sets the WKT projection of the dataset. May be empty.
func (ds *Dataset) SetProjection(wkt string) error {
	var cwkt = (*C.char)(nil)
	if len(wkt) > 0 {
		cwkt = C.CString(wkt)
		defer C.free(unsafe.Pointer(cwkt))
	}
	errmsg := C.godalSetProjection(ds.Handle(), cwkt)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// SpatialRef returns dataset projection.
func (ds *Dataset) SpatialRef() *SpatialRef {
	hndl := C.GDALGetSpatialRef(ds.Handle())
	return &SpatialRef{handle: hndl, isOwned: false}
}

// SetSpatialRef sets dataset's projection.
// sr can be set to nil to clear an existing projection
func (ds *Dataset) SetSpatialRef(sr *SpatialRef) error {
	var hndl C.OGRSpatialReferenceH
	if sr == nil {
		hndl = nil
	} else {
		hndl = sr.handle
	}
	errmsg := C.godalDatasetSetSpatialRef(ds.Handle(), hndl)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// GeoTransform returns the affine transformation coefficients
func (ds *Dataset) GeoTransform() ([6]float64, error) {
	ret := [6]float64{}
	gt := make([]C.double, 6)
	cgt := (*C.double)(unsafe.Pointer(&gt[0]))
	errmsg := C.godalGetGeoTransform(ds.Handle(), cgt)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return ret, errors.New(C.GoString(errmsg))
	}
	for i := range ret {
		ret[i] = float64(gt[i])
	}
	return ret, nil
}

// SetGeoTransform sets the affine transformation coefficients
func (ds *Dataset) SetGeoTransform(transform [6]float64) error {
	gt := cDoubleArray(transform[:])
	errmsg := C.godalSetGeoTransform(ds.Handle(), gt)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

//SetNoData sets the band's nodata value
func (ds *Dataset) SetNoData(nd float64) error {
	errmsg := C.godalSetDatasetNoDataValue(ds.Handle(), C.double(nd))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

type dsTranslateOpts struct {
	config   []string
	creation []string
	driver   DriverName
}

// DatasetTranslateOption is an option that can be passed to Dataset.Translate()
//
// Available DatasetTranslateOptions are:
//
// • ConfigOption
//
// • CreationOption
//
// • DriverName
type DatasetTranslateOption interface {
	setDatasetTranslateOpt(dto *dsTranslateOpts)
}

// Translate runs the library version of gdal_translate.
// See the gdal_translate doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//  []string{
//    "-a_nodata", 0,
//    "-a_srs", "epsg:4326"}
//
// Creation options and driver may be set either in the switches slice with
//  switches:=[]string{"-co","TILED=YES","-of","GTiff"}
// or through Options with
//  ds.Translate(dst, switches, CreationOption("TILED=YES","BLOCKXSIZE=256"), GTiff)
//
func (ds *Dataset) Translate(dstDS string, switches []string, opts ...DatasetTranslateOption) (*Dataset, error) {
	gopts := dsTranslateOpts{}
	for _, opt := range opts {
		opt.setDatasetTranslateOpt(&gopts)
	}
	for _, copt := range gopts.creation {
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
	hndl := C.godalTranslate((*C.char)(cname), ds.Handle(), cswitches.cPointer(), &errmsg, cconfig.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

type dsWarpOpts struct {
	config   []string
	creation []string
	driver   DriverName
}

// DatasetWarpOption is an option that can be passed to Dataset.Warp()
//
// Available DatasetWarpOptions are:
//
// • ConfigOption
//
// • CreationOption
//
// • DriverName
type DatasetWarpOption interface {
	setDatasetWarpOpt(dwo *dsWarpOpts)
}

// Warp runs the library version of gdalwarp
// See the gdalwarp doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//  []string{
//	  "-t_srs","epsg:3857",
//    "-dstalpha"}
//
// Creation options and driver may be set either in the switches slice with
//  switches:=[]string{"-co","TILED=YES","-of","GTiff"}
// or through Options with
//  ds.Warp(dst, switches, CreationOption("TILED=YES","BLOCKXSIZE=256"), GTiff)
//
func (ds *Dataset) Warp(dstDS string, switches []string, opts ...DatasetWarpOption) (*Dataset, error) {
	return Warp(dstDS, []*Dataset{ds}, switches, opts...)
}

// Warp writes provided sourceDS Datasets into new dataset and runs the library version of gdalwarp
// See the gdalwarp doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//  []string{
//	  "-t_srs","epsg:3857",
//    "-dstalpha"}
//
// Creation options and driver may be set either in the switches slice with
//  switches:=[]string{"-co","TILED=YES","-of","GTiff"}
// or through Options with
//  ds.Warp(dst, switches, CreationOption("TILED=YES","BLOCKXSIZE=256"), GTiff)
func Warp(dstDS string, sourceDS []*Dataset, switches []string, opts ...DatasetWarpOption) (*Dataset, error) {
	gopts := dsWarpOpts{}
	for _, opt := range opts {
		opt.setDatasetWarpOpt(&gopts)
	}

	for _, copt := range gopts.creation {
		switches = append(switches, "-co", copt)
	}

	if gopts.driver != "" {
		dname := string(gopts.driver)
		if dm, ok := driverMappings[gopts.driver]; ok {
			dname = dm.rasterName
		}
		switches = append(switches, "-of", dname)
	}

	srcDS := make([]C.GDALDatasetH, len(sourceDS))
	for i, dataset := range sourceDS {
		srcDS[i] = dataset.Handle()
	}

	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()
	cconfig := sliceToCStringArray(gopts.config)
	defer cconfig.free()
	cname := unsafe.Pointer(C.CString(dstDS))
	defer C.free(cname)

	var errmsg *C.char
	hndl := C.godalDatasetWarp((*C.char)(cname), C.int(len(sourceDS)), (*C.GDALDatasetH)(unsafe.Pointer(&srcDS[0])), cswitches.cPointer(), &errmsg, cconfig.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}

	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// DatasetWarpIntoOption is an option that can be passed to Dataset.WarpInto()
//
// Available DatasetWarpIntoOption is:
//
// • ConfigOption
//
type DatasetWarpIntoOption interface {
	setDatasetWarpIntoOpt(dwo *dsWarpIntoOpts)
}

type dsWarpIntoOpts struct {
	config []string
}

// WarpInto writes provided sourceDS Datasets into self existing dataset and runs the library version of gdalwarp
// See the gdalwarp doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//  []string{
//	  "-t_srs","epsg:3857",
//    "-dstalpha"}
func (ds *Dataset) WarpInto(sourceDS []*Dataset, switches []string, opts ...DatasetWarpIntoOption) error {
	gopts := dsWarpIntoOpts{}
	for _, opt := range opts {
		opt.setDatasetWarpIntoOpt(&gopts)
	}

	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()
	cconfig := sliceToCStringArray(gopts.config)
	defer cconfig.free()

	dstDS := ds.Handle()
	srcDS := make([]C.GDALDatasetH, len(sourceDS))
	for i, dataset := range sourceDS {
		srcDS[i] = dataset.Handle()
	}

	if errmsg := C.godalDatasetWarpInto(
		dstDS,
		C.int(len(sourceDS)),
		(*C.GDALDatasetH)(unsafe.Pointer(&srcDS[0])),
		cswitches.cPointer(),
		cconfig.cPointer(),
	); errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

type buildOvrOpts struct {
	config     []string
	minSize    int
	resampling ResamplingAlg
	bands      []int
	levels     []int
}

// BuildOverviewsOption is an option to specify how overview building should behave.
//
// Available BuildOverviewsOptions are:
//
// • ConfigOption
//
// • Resampling
//
// • Levels
//
// • MinSize
//
// • Bands
type BuildOverviewsOption interface {
	setBuildOverviewsOpt(bo *buildOvrOpts)
}

// BuildOverviews computes overviews for the dataset.
//
// If neither Levels() or MinSize() is specified, will compute overview
// levels such that the smallest overview is just under the block size.
//
// Not Setting OvrLevels() or OvrMinSize() if the dataset is not internally tiled
// is not an error but will probably not create the expected result (i.e. only a
// single overview will be created).
func (ds *Dataset) BuildOverviews(opts ...BuildOverviewsOption) error {
	bands := ds.Bands()
	if len(bands) == 0 {
		return fmt.Errorf("cannot compute overviews on dataset with no raster bands")
	}
	oopts := buildOvrOpts{
		resampling: Average,
	}

	structure := bands[0].Structure()

	//default size is to stop when just under the blocksize (so the band contains a single block)
	if structure.BlockSizeX > structure.BlockSizeY {
		oopts.minSize = structure.BlockSizeX
	} else {
		oopts.minSize = structure.BlockSizeY
	}

	for _, opt := range opts {
		opt.setBuildOverviewsOpt(&oopts)
	}

	if len(oopts.levels) == 0 { //levels need to be computed automatically
		lvl := 1
		sx, sy := structure.SizeX, structure.SizeY
		for sx > oopts.minSize || sy > oopts.minSize {
			lvl *= 2
			oopts.levels = append(oopts.levels, lvl)
			sx /= 2
			sy /= 2
		}
	}
	if len(oopts.levels) == 0 {
		return nil //nothing to do
	}
	for _, l := range oopts.levels {
		if l < 2 {
			return fmt.Errorf("cannot compute overview of level %d", l)
		}
	}
	nLevels := C.int(len(oopts.levels))
	cLevels := cIntArray(oopts.levels)
	nBands := C.int(len(oopts.bands))
	cBands := (*C.int)(nil)
	if nBands > 0 {
		cBands = cIntArray(oopts.bands)
	}
	copts := sliceToCStringArray(oopts.config)
	defer copts.free()
	cResample := unsafe.Pointer(C.CString(oopts.resampling.String()))
	defer C.free(cResample)

	errmsg := C.godalBuildOverviews(ds.Handle(), (*C.char)(cResample), nLevels, cLevels,
		nBands, cBands, copts.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// ClearOverviews deletes all dataset overviews
func (ds *Dataset) ClearOverviews() error {
	errmsg := C.godalClearOverviews(ds.Handle())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// Structure returns the dataset's Structure
func (ds *Dataset) Structure() DatasetStructure {
	var sx, sy, bsx, bsy, bandCount, dtype C.int
	C.godalDatasetStructure(ds.Handle(), &sx, &sy, &bsx, &bsy, &bandCount, &dtype)
	return DatasetStructure{
		BandStructure: BandStructure{
			SizeX:      int(sx),
			SizeY:      int(sy),
			BlockSizeX: int(bsx),
			BlockSizeY: int(bsy),
			DataType:   DataType(int(dtype)),
		},
		NBands: int(bandCount),
	}
}

type datasetIOOpt struct {
	config                                 []string
	bands                                  []int
	dsWidth, dsHeight                      int
	resampling                             ResamplingAlg
	bandInterleave                         bool //return r1r2...rn,g1g2...gn,b1b2...bn instead of r1g1b1,r2g2b2,...,rngnbn
	bandSpacing, pixelSpacing, lineSpacing int
}

// DatasetIOOption is an option to modify the default behavior of dataset.IO
//
// Available DatasetIOOptions are:
//
// • Stride
//
// • Window
//
// • Resampling
//
// • ConfigOption
//
// • Bands
//
// • BandInterleaved
//
// • PixelSpacing
//
// • LineSpacing
//
// • BandSpacing
type DatasetIOOption interface {
	setDatasetIOOpt(ro *datasetIOOpt)
}

// Read populates the supplied buffer with the pixels contained in the supplied window
func (ds *Dataset) Read(srcX, srcY int, buffer interface{}, bufWidth, bufHeight int, opts ...DatasetIOOption) error {
	return ds.IO(IORead, srcX, srcY, buffer, bufWidth, bufHeight, opts...)
}

// Write sets the dataset's pixels contained in the supplied window to the content of the supplied buffer
func (ds *Dataset) Write(srcX, srcY int, buffer interface{}, bufWidth, bufHeight int, opts ...DatasetIOOption) error {
	return ds.IO(IOWrite, srcX, srcY, buffer, bufWidth, bufHeight, opts...)
}

// IO reads or writes the pixels contained in the supplied window
func (ds *Dataset) IO(rw IOOperation, srcX, srcY int, buffer interface{}, bufWidth, bufHeight int, opts ...DatasetIOOption) error {
	var bands []Band
	ro := datasetIOOpt{}
	for _, opt := range opts {
		opt.setDatasetIOOpt(&ro)
	}
	if ro.dsHeight == 0 {
		ro.dsHeight = bufHeight
	}
	if ro.dsWidth == 0 {
		ro.dsWidth = bufWidth
	}
	if ro.bands == nil {
		bands = ds.Bands()
		if len(bands) == 0 {
			return fmt.Errorf("cannot perform io on dataset with no bands")
		}
		for i := range bands {
			ro.bands = append(ro.bands, i+1)
		}
	}
	dsize, dtype, cBuf := cBuffer(buffer)
	pixelSpacing := C.int(dsize * len(ro.bands))
	lineSpacing := C.int(bufWidth * dsize * len(ro.bands))
	bandSpacing := C.int(dsize)
	if ro.bandInterleave {
		pixelSpacing = C.int(dsize)
		lineSpacing = C.int(bufWidth * dsize)
		bandSpacing = C.int(bufHeight * bufWidth * dsize)
	}
	if ro.pixelSpacing > 0 {
		pixelSpacing = C.int(ro.pixelSpacing)
	}
	if ro.bandSpacing > 0 {
		bandSpacing = C.int(ro.bandSpacing)
	}
	if ro.lineSpacing > 0 {
		lineSpacing = C.int(ro.lineSpacing)
	}

	ralg, err := ro.resampling.rioAlg()
	if err != nil {
		return err
	}
	configOpts := sliceToCStringArray(ro.config)
	defer configOpts.free()
	//fmt.Fprintf(os.Stderr, "%v %d %d %d\n", ro.bands, pixelSpacing, lineSpacing, bandSpacing)

	errmsg := C.godalDatasetRasterIO(ds.Handle(), C.GDALRWFlag(rw),
		C.int(srcX), C.int(srcY), C.int(ro.dsWidth), C.int(ro.dsHeight),
		cBuf,
		C.int(bufWidth), C.int(bufHeight), C.GDALDataType(dtype),
		C.int(len(ro.bands)), cIntArray(ro.bands),
		pixelSpacing, lineSpacing, bandSpacing, ralg,
		configOpts.cPointer())

	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}
