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
#include "godal.h"
#include <stdlib.h>

#cgo pkg-config: gdal
#cgo LDFLAGS: -ldl
*/
import "C"
import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/airbusgeo/osio"
)

// DataType is a pixel data types
type DataType int

const (
	//Unknown / Unset Datatype
	Unknown = DataType(C.GDT_Unknown)
	//Byte / UInt8
	Byte = DataType(C.GDT_Byte)
	//UInt16 DataType
	UInt16 = DataType(C.GDT_UInt16)
	//Int16 DataType
	Int16 = DataType(C.GDT_Int16)
	//UInt32 DataType
	UInt32 = DataType(C.GDT_UInt32)
	//Int32 DataType
	Int32 = DataType(C.GDT_Int32)
	//Float32 DataType
	Float32 = DataType(C.GDT_Float32)
	//Float64 DataType
	Float64 = DataType(C.GDT_Float64)
	//CInt16 is a complex Int16
	CInt16 = DataType(C.GDT_CInt16)
	//CInt32 is a complex Int32
	CInt32 = DataType(C.GDT_CInt32)
	//CFloat32 is a complex Float32
	CFloat32 = DataType(C.GDT_CFloat32)
	//CFloat64 is a complex Float64
	CFloat64 = DataType(C.GDT_CFloat64)
)

type ErrorCategory int

const (
	CE_None    = ErrorCategory(C.CE_None)
	CE_Debug   = ErrorCategory(C.CE_Debug)
	CE_Warning = ErrorCategory(C.CE_Warning)
	CE_Failure = ErrorCategory(C.CE_Failure)
	CE_Fatal   = ErrorCategory(C.CE_Fatal)
)

// String implements Stringer
func (dtype DataType) String() string {
	return C.GoString(C.GDALGetDataTypeName(C.GDALDataType(dtype)))
}

// Size retruns the number of bytes needed for one instance of DataType
func (dtype DataType) Size() int {
	switch dtype {
	case Byte:
		return 1
	case Int16, UInt16:
		return 2
	case Int32, UInt32, Float32, CInt16:
		return 4
	case CInt32, Float64, CFloat32:
		return 8
	case CFloat64:
		return 16
	default:
		panic("unsupported type")
	}
}

//ColorInterp is a band's color interpretation
type ColorInterp int

const (
	//CIUndefined is an undefined ColorInterp
	CIUndefined = ColorInterp(C.GCI_Undefined)
	//CIGray is a gray level ColorInterp
	CIGray = ColorInterp(C.GCI_GrayIndex)
	//CIPalette is an undefined ColorInterp
	CIPalette = ColorInterp(C.GCI_PaletteIndex)
	//CIRed is a paletted ColorInterp
	CIRed = ColorInterp(C.GCI_RedBand)
	//CIGreen is a Green ColorInterp
	CIGreen = ColorInterp(C.GCI_GreenBand)
	//CIBlue is a Blue ColorInterp
	CIBlue = ColorInterp(C.GCI_BlueBand)
	//CIAlpha is an Alpha/Transparency ColorInterp
	CIAlpha = ColorInterp(C.GCI_AlphaBand)
	//CIHue is an HSL Hue ColorInterp
	CIHue = ColorInterp(C.GCI_HueBand)
	//CISaturation is an HSL Saturation ColorInterp
	CISaturation = ColorInterp(C.GCI_SaturationBand)
	//CILightness is an HSL Lightness ColorInterp
	CILightness = ColorInterp(C.GCI_LightnessBand)
	//CICyan is an CMYK Cyan ColorInterp
	CICyan = ColorInterp(C.GCI_CyanBand)
	//CIMagenta is an CMYK Magenta ColorInterp
	CIMagenta = ColorInterp(C.GCI_MagentaBand)
	//CIYellow is an CMYK Yellow ColorInterp
	CIYellow = ColorInterp(C.GCI_YellowBand)
	//CIBlack is an CMYK Black ColorInterp
	CIBlack = ColorInterp(C.GCI_BlackBand)
	//CIY is a YCbCr Y ColorInterp
	CIY = ColorInterp(C.GCI_YCbCr_YBand)
	//CICb is a YCbCr Cb ColorInterp
	CICb = ColorInterp(C.GCI_YCbCr_CbBand)
	//CICr is a YCbCr Cr ColorInterp
	CICr = ColorInterp(C.GCI_YCbCr_CrBand)
	//CIMax is an maximum ColorInterp
	CIMax = ColorInterp(C.GCI_Max)
)

// Name returns the ColorInterp's name
func (colorInterp ColorInterp) Name() string {
	return C.GoString(C.GDALGetColorInterpretationName(C.GDALColorInterp(colorInterp)))
}

// Band is a wrapper around a GDALRasterBandH
type Band struct {
	majorObject
}

// handle() returns a pointer to the underlying GDALRasterBandH
func (band Band) handle() C.GDALRasterBandH {
	return C.GDALRasterBandH(band.majorObject.cHandle)
}

// Structure returns the dataset's Structure
func (band Band) Structure() BandStructure {
	var sx, sy, bsx, bsy, dtype C.int
	C.godalBandStructure(band.handle(), &sx, &sy, &bsx, &bsy, &dtype)
	return BandStructure{
		SizeX:      int(sx),
		SizeY:      int(sy),
		BlockSizeX: int(bsx),
		BlockSizeY: int(bsy),
		DataType:   DataType(int(dtype)),
	}
}

//NoData returns the band's nodata value. if ok is false, the band does not
//have a nodata value set
func (band Band) NoData() (nodata float64, ok bool) {
	cok := C.int(0)
	cn := C.GDALGetRasterNoDataValue(band.handle(), &cok)
	if cok != 0 {
		return float64(cn), true
	}
	return 0, false
}

//SetNoData sets the band's nodata value
func (band Band) SetNoData(nd float64) error {
	errmsg := C.godalSetRasterNoDataValue(band.handle(), C.double(nd))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// ClearNoData clears the band's nodata value
func (band Band) ClearNoData() error {
	errmsg := C.godalDeleteRasterNoDataValue(band.handle())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// ColorInterp returns the band's color interpretation (defaults to Gray)
func (band Band) ColorInterp() ColorInterp {
	colorInterp := C.GDALGetRasterColorInterpretation(band.handle())
	return ColorInterp(colorInterp)
}

// SetColorInterp sets the band's color interpretation
func (band Band) SetColorInterp(colorInterp ColorInterp) error {
	errmsg := C.godalSetRasterColorInterpretation(band.handle(), C.GDALColorInterp(colorInterp))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

//MaskFlags returns the mask flags associated with this band.
//See https://gdal.org/development/rfc/rfc15_nodatabitmask.html for how this flag
//should be interpreted
func (band Band) MaskFlags() int {
	return int(C.GDALGetMaskFlags(band.handle()))
}

//MaskBand returns the mask (nodata) band for this band. May be generated from nodata values.
func (band Band) MaskBand() Band {
	hndl := C.GDALGetMaskBand(band.handle())
	return Band{majorObject{C.GDALMajorObjectH(hndl)}}
}

//CreateMask creates a mask (nodata) band for this band.
//Any handle returned by a previous call to MaskBand() should not be used after a call to CreateMask
//See https://gdal.org/development/rfc/rfc15_nodatabitmask.html for how flag should be used
func (band Band) CreateMask(flags int, opts ...BandCreateMaskOption) (Band, error) {
	gopts := bandCreateMaskOpts{}
	for _, opt := range opts {
		opt.setBandCreateMaskOpt(&gopts)
	}
	cconfig := sliceToCStringArray(gopts.config)
	defer cconfig.free()

	var errmsg *C.char
	hndl := C.godalCreateMaskBand(band.handle(), C.int(flags), &errmsg, cconfig.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return Band{}, errors.New(C.GoString(errmsg))
	}
	return Band{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

//Fill sets the whole band uniformely to (real,imag)
func (band Band) Fill(real, imag float64) error {
	errmsg := C.godalFillRaster(band.handle(), C.double(real), C.double(imag))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// Read populates the supplied buffer with the pixels contained in the supplied window
func (band Band) Read(srcX, srcY int, buffer interface{}, bufWidth, bufHeight int, opts ...BandIOOption) error {
	return band.IO(IORead, srcX, srcY, buffer, bufWidth, bufHeight, opts...)
}

// Write sets the dataset's pixels contained in the supplied window to the content of the supplied buffer
func (band Band) Write(srcX, srcY int, buffer interface{}, bufWidth, bufHeight int, opts ...BandIOOption) error {
	return band.IO(IOWrite, srcX, srcY, buffer, bufWidth, bufHeight, opts...)
}

// IO reads or writes the pixels contained in the supplied window
func (band Band) IO(rw IOOperation, srcX, srcY int, buffer interface{}, bufWidth, bufHeight int, opts ...BandIOOption) error {
	ro := bandIOOpt{}
	for _, opt := range opts {
		opt.setBandIOOpt(&ro)
	}
	if ro.dsHeight == 0 {
		ro.dsHeight = bufHeight
	}
	if ro.dsWidth == 0 {
		ro.dsWidth = bufWidth
	}
	dsize, dtype, cBuf := cBuffer(buffer)
	pixelSpacing := C.int(dsize)
	if ro.pixelSpacing > 0 {
		pixelSpacing = C.int(ro.pixelSpacing)
	}
	lineSpacing := C.int(bufWidth) * pixelSpacing
	if ro.lineSpacing > 0 {
		lineSpacing = C.int(ro.lineSpacing)
	}
	//fmt.Fprintf(os.Stderr, "%v %d %d %d\n", ro.bands, pixelSpacing, lineSpacing, bandSpacing)
	ralg, err := ro.resampling.rioAlg()
	if err != nil {
		return err
	}
	configOpts := sliceToCStringArray(ro.config)
	defer configOpts.free()

	errmsg := C.godalBandRasterIO(band.handle(), C.GDALRWFlag(rw),
		C.int(srcX), C.int(srcY), C.int(ro.dsWidth), C.int(ro.dsHeight),
		cBuf,
		C.int(bufWidth), C.int(bufHeight), C.GDALDataType(dtype),
		pixelSpacing, lineSpacing, ralg,
		configOpts.cPointer())

	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// Polygonize wraps GDALPolygonize
func (band Band) Polygonize(dstLayer Layer, opts ...PolygonizeOption) error {
	popt := polygonizeOpt{
		pixFieldIndex: -1,
	}
	maskBand := band.MaskBand()
	popt.mask = &maskBand

	for _, opt := range opts {
		opt.setPolygonizeOpt(&popt)
	}
	copts := sliceToCStringArray(popt.options)
	defer copts.free()
	var cMaskBand C.GDALRasterBandH = nil
	if popt.mask != nil {
		cMaskBand = popt.mask.handle()
	}

	errmsg := C.godalPolygonize(band.handle(), cMaskBand, dstLayer.handle(), C.int(popt.pixFieldIndex), copts.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

//Overviews returns all overviews of band
func (band Band) Overviews() []Band {
	cbands := C.godalBandOverviews(band.handle())
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

//Histogram returns or computes the bands histogram
func (band Band) Histogram(opts ...HistogramOption) (Histogram, error) {
	hopt := histogramOpts{}
	for _, o := range opts {
		o.setHistogramOpt(&hopt)
	}
	var values *C.ulonglong = nil
	defer C.VSIFree(unsafe.Pointer(values))

	errmsg := C.godalRasterHistogram(band.handle(), (*C.double)(&hopt.min), (*C.double)(&hopt.max), (*C.int)(&hopt.buckets),
		&values, C.int(hopt.includeOutside), C.int(hopt.approx))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return Histogram{}, errors.New(C.GoString(errmsg))
	}
	counts := (*[1 << 30]C.ulonglong)(unsafe.Pointer(values))
	h := Histogram{
		min:    hopt.min,
		max:    hopt.max,
		counts: make([]uint64, hopt.buckets),
	}
	for i := int32(0); i < hopt.buckets; i++ {
		h.counts[i] = uint64(counts[i])
	}
	return h, nil
}

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

//PaletteInterp defines the color interpretation of a ColorTable
type PaletteInterp C.GDALPaletteInterp

const (
	//GrayscalePalette is a grayscale palette with a single component per entry
	GrayscalePalette PaletteInterp = C.GPI_Gray
	//RGBPalette is a RGBA palette with 4 components per entry
	RGBPalette PaletteInterp = C.GPI_RGB
	//CMYKPalette is a CMYK palette with 4 components per entry
	CMYKPalette PaletteInterp = C.GPI_CMYK
	//HLSPalette is a HLS palette with 3 components per entry
	HLSPalette PaletteInterp = C.GPI_HLS
)

//ColorTable is a color table associated with a Band
type ColorTable struct {
	PaletteInterp PaletteInterp
	Entries       [][4]int16
}

func cColorTableArray(in [][4]int16) *C.short {
	ret := make([]C.short, len(in)*4)
	for i := range in {
		ret[4*i] = C.short(in[i][0])
		ret[4*i+1] = C.short(in[i][1])
		ret[4*i+2] = C.short(in[i][2])
		ret[4*i+3] = C.short(in[i][3])
	}
	return (*C.short)(unsafe.Pointer(&ret[0]))
}

func ctEntriesFromCshorts(arr *C.short, nEntries int) [][4]int16 {
	int16s := (*[1 << 30]C.short)(unsafe.Pointer(arr))
	ret := make([][4]int16, nEntries)
	for i := 0; i < nEntries; i++ {
		ret[i][0] = int16(int16s[i*4])
		ret[i][1] = int16(int16s[i*4+1])
		ret[i][2] = int16(int16s[i*4+2])
		ret[i][3] = int16(int16s[i*4+3])
	}
	return ret
}

//ColorTable returns the bands color table. The returned ColorTable will have
//a 0-length Entries if the band has no color table assigned
func (band Band) ColorTable() ColorTable {
	var interp C.GDALPaletteInterp
	var nEntries C.int
	var cEntries *C.short
	C.godalGetColorTable(band.handle(), &interp, &nEntries, &cEntries)
	if cEntries != nil {
		defer C.free(unsafe.Pointer(cEntries))
	}
	return ColorTable{
		PaletteInterp: PaletteInterp(interp),
		Entries:       ctEntriesFromCshorts(cEntries, int(nEntries)),
	}
}

// SetColorTable sets the band's color table. if passing in a 0-length ct.Entries,
// the band's color table will be cleared
func (band Band) SetColorTable(ct ColorTable) error {
	var cshorts *C.short
	if len(ct.Entries) > 0 {
		cshorts = cColorTableArray(ct.Entries)
	}
	errmsg := C.godalSetColorTable(band.handle(), C.GDALPaletteInterp(ct.PaletteInterp), C.int(len(ct.Entries)), cshorts)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// Bands returns all dataset bands.
func (ds *Dataset) Bands() []Band {
	cbands := C.godalRasterBands(ds.handle())
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
	hndl := C.godalCreateDatasetMaskBand(ds.handle(), C.int(flags), &errmsg, cconfig.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return Band{}, errors.New(C.GoString(errmsg))
	}
	return Band{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// Projection returns the WKT projection of the dataset. May be empty.
func (ds *Dataset) Projection() string {
	str := C.GDALGetProjectionRef(ds.handle())
	return C.GoString(str)
}

// SetProjection sets the WKT projection of the dataset. May be empty.
func (ds *Dataset) SetProjection(wkt string) error {
	var cwkt = (*C.char)(nil)
	if len(wkt) > 0 {
		cwkt = C.CString(wkt)
		defer C.free(unsafe.Pointer(cwkt))
	}
	errmsg := C.godalSetProjection(ds.handle(), cwkt)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// SpatialRef returns dataset projection.
func (ds *Dataset) SpatialRef() *SpatialRef {
	hndl := C.GDALGetSpatialRef(ds.handle())
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
	errmsg := C.godalDatasetSetSpatialRef(ds.handle(), hndl)
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
	errmsg := C.godalGetGeoTransform(ds.handle(), cgt)
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
	errmsg := C.godalSetGeoTransform(ds.handle(), gt)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

//SetNoData sets the band's nodata value
func (ds *Dataset) SetNoData(nd float64) error {
	errmsg := C.godalSetDatasetNoDataValue(ds.handle(), C.double(nd))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
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
	hndl := C.godalTranslate((*C.char)(cname), ds.handle(), cswitches.cPointer(), &errmsg, cconfig.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
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
		srcDS[i] = dataset.handle()
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

	dstDS := ds.handle()
	srcDS := make([]C.GDALDatasetH, len(sourceDS))
	for i, dataset := range sourceDS {
		srcDS[i] = dataset.handle()
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

	errmsg := C.godalBuildOverviews(ds.handle(), (*C.char)(cResample), nLevels, cLevels,
		nBands, cBands, copts.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// ClearOverviews deletes all dataset overviews
func (ds *Dataset) ClearOverviews() error {
	errmsg := C.godalClearOverviews(ds.handle())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// Structure returns the dataset's Structure
func (ds *Dataset) Structure() DatasetStructure {
	var sx, sy, bsx, bsy, bandCount, dtype C.int
	C.godalDatasetStructure(ds.handle(), &sx, &sy, &bsx, &bsy, &bandCount, &dtype)
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

	errmsg := C.godalDatasetRasterIO(ds.handle(), C.GDALRWFlag(rw),
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

// RegisterAll calls GDALAllRegister which registers all available raster and vector
// drivers.
//
// Alternatively, you may also register a select number of drivers by calling one or more of
//  godal.RegisterInternal() // MEM, VRT, GTiff and GeoJSON
//  godal.RegisterRaster(godal.GTiff,godal.VRT)
//  godal.RegisterVector(godal.Shapefile)
func RegisterAll() {
	C.GDALAllRegister()
}

// RegisterRaster registers a raster driver by name.
//
// Calling RegisterRaster(DriverName) with one of the predefined DriverNames provided by the library will
// register the corresponding raster driver.
//
// Calling RegisterRaster(DriverName("XXX")) with "XXX" any string will result in calling the function
// GDALRegister_XXX() if it could be found inside the ligdal.so binary. This allows to register any raster driver
// known to gdal but not explicitly defined inside this golang wrapper. Note that "XXX" must be provided
// exactly (i.e. respecting uppercase/lowercase) the same as the names of the C functions GDALRegister_XXX()
// that can be found in gdal.h
func RegisterRaster(drivers ...DriverName) error {
	for _, driver := range drivers {
		switch driver {
		case Memory:
			C.GDALRegister_MEM()
		case VRT:
			C.GDALRegister_VRT()
		case HFA:
			C.GDALRegister_HFA()
		case GTiff:
			C.GDALRegister_GTiff()
		default:
			fnname := fmt.Sprintf("GDALRegister_%s", driver)
			drv, ok := driverMappings[driver]
			if ok {
				fnname = drv.rasterRegister
			}
			if fnname == "" {
				return fmt.Errorf("%s driver does not handle rasters", fnname)
			}
			if err := registerDriver(fnname); err != nil {
				return err
			}
		}
	}
	return nil
}

// RegisterVector registers a vector driver by name.
//
// Calling RegisterVector(DriverName) with one of the predefined DriverNames provided by the library will
// register the corresponding vector driver.
//
// Calling RegisterVector(DriverName("XXX")) with "XXX" any string will result in calling the function
// RegisterOGRXXX() if it could be found inside the ligdal.so binary. This allows to register any vector driver
// known to gdal but not explicitly defined inside this golang wrapper. Note that "XXX" must be provided
// exactly (i.e. respecting uppercase/lowercase) the same as the names of the C functions RegisterOGRXXX()
// that can be found in ogrsf_frmts.h
func RegisterVector(drivers ...DriverName) error {
	for _, driver := range drivers {
		switch driver {
		/* TODO: speedup for OGR drivers
		case VRT:
			C.RegisterOGRVRT()
		case Memory:
			C.RegisterOGRMEM()
		case Mitab:
			C.RegisterOGRTAB()
		case GeoJSON:
			C.RegisterOGRGeoJSON()
		*/
		default:
			fnname := fmt.Sprintf("RegisterOGR%s", driver)
			drv, ok := driverMappings[driver]
			if ok {
				fnname = drv.vectorRegister
			}
			if fnname == "" {
				return fmt.Errorf("%s driver does not handle vectors", fnname)
			}
			if err := registerDriver(fnname); err != nil {
				return err
			}
		}
	}
	return nil
}

func registerDriver(fnname string) error {
	cfnname := C.CString(fnname)
	defer C.free(unsafe.Pointer(cfnname))
	ret := C.godalRegisterDriver(cfnname)
	if ret != 0 {
		return fmt.Errorf("failed to call function %s", fnname)
	}
	return nil
}

// RegisterInternalDrivers is a shorthand for registering "essential" gdal/ogr drivers.
//
// It is equivalent to calling RegisterRaster("VRT","MEM","GTiff") and
// RegisterVector("MEM","VRT","GeoJSON")
func RegisterInternalDrivers() {
	//These are always build in and should never error
	_ = RegisterRaster(VRT, Memory, GTiff)
	_ = RegisterVector(VRT, Memory, GeoJSON)
}

// Driver is a gdal format driver
type Driver struct {
	majorObject
}

// handle() returns a pointer to the underlying GDALDriverH
func (drv Driver) handle() C.GDALDriverH {
	return C.GDALDriverH(drv.majorObject.cHandle)
}

// VectorDriver returns a Driver by name. It returns false if the named driver does
// not exist
func VectorDriver(name DriverName) (Driver, bool) {
	if dn, ok := driverMappings[name]; ok {
		if dn.vectorName == "" {
			return Driver{}, false
		}
		return getDriver(dn.vectorName)
	}
	return getDriver(string(name))
}

// RasterDriver returns a Driver by name. It returns false if the named driver does
// not exist
func RasterDriver(name DriverName) (Driver, bool) {
	if dn, ok := driverMappings[name]; ok {
		if dn.rasterName == "" {
			return Driver{}, false
		}
		return getDriver(dn.rasterName)
	}
	return getDriver(string(name))
}

func getDriver(name string) (Driver, bool) {
	cname := C.CString(string(name))
	defer C.free(unsafe.Pointer(cname))
	hndl := C.GDALGetDriverByName((*C.char)(unsafe.Pointer(cname)))
	if hndl != nil {
		return Driver{majorObject{C.GDALMajorObjectH(hndl)}}, true
	}
	return Driver{}, false
}

// Create wraps GDALCreate and uses driver to creates a new raster dataset with the given name (usually filename), size, type and bands.
func Create(driver DriverName, name string, nBands int, dtype DataType, width, height int, opts ...DatasetCreateOption) (*Dataset, error) {
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
	configOpts := sliceToCStringArray(gopts.config)
	cname := C.CString(name)
	defer createOpts.free()
	defer configOpts.free()
	defer C.free(unsafe.Pointer(cname))
	var errmsg *C.char
	hndl := C.godalCreate(drv.handle(), (*C.char)(unsafe.Pointer(cname)),
		C.int(width), C.int(height), C.int(nBands), C.GDALDataType(dtype),
		createOpts.cPointer(), &errmsg, configOpts.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil

}

// CreateVector wraps GDALCreate and uses driver to create a new vector dataset with the given name
// (usually filename) and options
func CreateVector(driver DriverName, name string, opts ...DatasetCreateOption) (*Dataset, error) {
	drvname := string(driver)
	if drv, ok := driverMappings[driver]; ok {
		if drv.vectorName == "" {
			return nil, fmt.Errorf("%s does not support vector creation", driver)
		}
		drvname = drv.vectorName
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
	configOpts := sliceToCStringArray(gopts.config)
	cname := C.CString(name)
	defer createOpts.free()
	defer configOpts.free()
	defer C.free(unsafe.Pointer(cname))
	var errmsg *C.char
	hndl := C.godalCreate(drv.handle(), (*C.char)(unsafe.Pointer(cname)),
		0, 0, 0, C.GDT_Unknown,
		createOpts.cPointer(), &errmsg, configOpts.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil

}

type majorObject struct {
	cHandle C.GDALMajorObjectH
}

//Dataset is a wrapper around a GDALDatasetH
type Dataset struct {
	majorObject
}

//handle returns a pointer to the underlying GDALDatasetH
func (ds *Dataset) handle() C.GDALDatasetH {
	return C.GDALDatasetH(ds.majorObject.cHandle)
}

//Open calls GDALOpenEx() with the provided options. It returns nil and an error
//in case there was an error opening the provided dataset name.
//name may be a filename or any supported string supported by gdal (e.g. a /vsixxx path,
//the xml string representing a vrt dataset, etc...)
func Open(name string, options ...OpenOption) (*Dataset, error) {
	oopts := openOptions{
		flags:        C.GDAL_OF_READONLY | C.GDAL_OF_VERBOSE_ERROR,
		siblingFiles: []string{filepath.Base(name)},
	}
	for _, opt := range options {
		opt.setOpenOption(&oopts)
	}
	csiblings := sliceToCStringArray(oopts.siblingFiles)
	coopts := sliceToCStringArray(oopts.options)
	cdrivers := sliceToCStringArray(oopts.drivers)
	defer csiblings.free()
	defer coopts.free()
	defer cdrivers.free()
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	cgc := createCGOContext(oopts.config, oopts.errorHandler)

	retds := C.godalOpen(cgc.cPointer(), cname, C.uint(oopts.flags),
		cdrivers.cPointer(), coopts.cPointer(), csiblings.cPointer())

	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(retds)}}, nil
}

//Close releases the dataset
func (ds *Dataset) Close() error {
	if ds.cHandle == nil {
		return fmt.Errorf("close called more than once")
	}
	var errmsg *C.char
	C.godalClose(ds.handle(), (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	ds.cHandle = nil
	return nil
}

//LibVersion is the GDAL lib versioning scheme
type LibVersion int

//Major returns the GDAL major version (e.g. "3" in 3.2.1)
func (lv LibVersion) Major() int {
	return int(lv) / 1000000
}

//Minor return the GDAL minor version (e.g. "2" in 3.2.1)
func (lv LibVersion) Minor() int {
	return (int(lv) - lv.Major()*1000000) / 10000
}

//Revision returns the GDAL revision number (e.g. "1" in 3.2.1)
func (lv LibVersion) Revision() int {
	return (int(lv) - lv.Major()*1000000 - lv.Minor()*10000) / 100
}

//AssertMinVersion will panic if the runtime version is not at least major.minor.revision
func AssertMinVersion(major, minor, revision int) {
	runtimeVersion := Version()
	if runtimeVersion.Major() < major ||
		(runtimeVersion.Major() == major && runtimeVersion.Minor() < minor) ||
		(runtimeVersion.Major() == major && runtimeVersion.Minor() == minor && runtimeVersion.Revision() < revision) {
		panic(fmt.Errorf("runtime version %d.%d.%d < %d.%d.%d",
			runtimeVersion.Major(), runtimeVersion.Minor(), runtimeVersion.Revision(), major, minor, revision))
	}
}

func init() {
	compiledVersion := LibVersion(C.GDAL_VERSION_NUM)
	AssertMinVersion(compiledVersion.Major(), compiledVersion.Minor(), 0)
}

//export goErrorHandler
func goErrorHandler(loggerID C.int, ec C.int, code C.int, msg *C.char) C.int {
	//returns 0 if the received ec/code/msg is not an actual error
	//returns !0 if msg should be considered an error
	lfn := getErrorHandler(int(loggerID))
	err := lfn.fn(ErrorCategory(ec), int(code), C.GoString(msg))
	if err != nil {
		lfn.errors = append(lfn.errors, err)
		return 1
	}
	return 0
}

func testErrorAndLogging(opts ...errorAndLoggingOption) error {
	ealo := errorAndLoggingOpts{}
	for _, o := range opts {
		o.setErrorAndLoggingOpt(&ealo)
	}
	cctx := createCGOContext(ealo.config, ealo.eh)

	C.test_godal_error_handling(cctx.cPointer())
	return cctx.close()
}

// Version returns the runtime version of the gdal library
func Version() LibVersion {
	cstr := C.CString("VERSION_NUM")
	defer C.free(unsafe.Pointer(cstr))
	version := C.GoString(C.GDALVersionInfo(cstr))
	iversion, _ := strconv.Atoi(version)
	return LibVersion(iversion)
}

// IOOperation determines wether Band.IO or Dataset.IO will read pixels into the
// provided buffer, or write pixels from the provided buffer
type IOOperation C.GDALRWFlag

const (
	//IORead makes IO copy pixels from the band/dataset into the provided buffer
	IORead IOOperation = C.GF_Read
	//IOWrite makes IO copy pixels from the provided buffer into the band/dataset
	IOWrite = C.GF_Write
)

//ResamplingAlg is a resampling method
type ResamplingAlg int

const (
	//Nearest resampling
	Nearest ResamplingAlg = iota
	// Bilinear resampling
	Bilinear
	// Cubic resampling
	Cubic
	// CubicSpline resampling
	CubicSpline
	// Lanczos resampling
	Lanczos
	// Average resampling
	Average
	// Gauss resampling
	Gauss
	// Mode resampling
	Mode
	// Max resampling
	Max
	// Min resampling
	Min
	// Median resampling
	Median
	// Sum resampling
	Sum
	// Q1 resampling
	Q1
	// Q3 resampling
	Q3
	//RMS gdal >=3.3
)

func (ra ResamplingAlg) String() string {
	switch ra {
	case Nearest:
		return "nearest"
	case Average:
		return "average"
	case Bilinear:
		return "bilinear"
	case Cubic:
		return "cubic"
	case CubicSpline:
		return "cubicspline"
	case Lanczos:
		return "lanczos"
	case Gauss:
		return "gauss"
	case Mode:
		return "mode"
	//case RMS:
	//	return "rms"
	case Q1:
		return "Q1"
	case Q3:
		return "Q3"
	case Median:
		return "med"
	case Max:
		return "max"
	case Min:
		return "min"
	case Sum:
		return "sum"
	default:
		panic("unsupported resampling")
	}
}

func (ra ResamplingAlg) rioAlg() (C.GDALRIOResampleAlg, error) {
	switch ra {
	case Nearest:
		return C.GRIORA_NearestNeighbour, nil
	case Average:
		return C.GRIORA_Average, nil
	case Bilinear:
		return C.GRIORA_Bilinear, nil
	case Cubic:
		return C.GRIORA_Cubic, nil
	case CubicSpline:
		return C.GRIORA_CubicSpline, nil
	case Lanczos:
		return C.GRIORA_Lanczos, nil
	case Gauss:
		return C.GRIORA_Gauss, nil
	case Mode:
		return C.GRIORA_Mode, nil
	//case RMS:
	//	return C.GRIORA_RMS, nil
	default:
		return C.GRIORA_NearestNeighbour, fmt.Errorf("%s resampling not supported for IO", ra.String())

	}
}

//cBuffer returns the byte size of an individual element, and a pointer to the
//underlying memory array
func cBuffer(buffer interface{}) (int, DataType, unsafe.Pointer) {
	var dtype DataType
	var cBuf unsafe.Pointer
	switch buf := buffer.(type) {
	case []byte:
		dtype = Byte
		cBuf = unsafe.Pointer(&buf[0])
	case []int16:
		dtype = Int16
		cBuf = unsafe.Pointer(&buf[0])
	case []uint16:
		dtype = UInt16
		cBuf = unsafe.Pointer(&buf[0])
	case []int32:
		dtype = Int32
		cBuf = unsafe.Pointer(&buf[0])
	case []uint32:
		dtype = UInt32
		cBuf = unsafe.Pointer(&buf[0])
	case []float32:
		dtype = Float32
		cBuf = unsafe.Pointer(&buf[0])
	case []float64:
		dtype = Float64
		cBuf = unsafe.Pointer(&buf[0])
	case []complex64:
		dtype = CFloat32
		cBuf = unsafe.Pointer(&buf[0])
	case []complex128:
		dtype = CFloat64
		cBuf = unsafe.Pointer(&buf[0])
	default:
		panic("unsupported type")
	}
	dsize := dtype.Size()
	return dsize, dtype, cBuf
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
	str := C.GDALGetMetadataItem(mo.cHandle, ckey, cdom)
	return C.GoString(str)
}

func (mo majorObject) Metadatas(opts ...MetadataOption) map[string]string {
	mopts := metadataOpt{}
	for _, opt := range opts {
		opt.setMetadataOpt(&mopts)
	}
	cdom := C.CString(mopts.domain)
	defer C.free(unsafe.Pointer(cdom))
	strs := C.GDALGetMetadata(mo.cHandle, cdom)
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
	errmsg := C.godalSetMetadataItem(mo.cHandle, ckey, cval, cdom)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

func (mo majorObject) MetadataDomains() []string {
	strs := C.GDALGetMetadataDomainList(mo.cHandle)
	return cStringArrayToSlice(strs)
}

type openUpdateOpt struct{}

//Update is an OpenOption that instructs gdal to open the dataset for writing/updating
func Update() interface {
	OpenOption
} {
	return openUpdateOpt{}
}

func (openUpdateOpt) setOpenOption(oo *openOptions) {
	//unset readonly
	oo.flags = oo.flags &^ C.GDAL_OF_READONLY //actually a noop as OF_READONLY is 0
	oo.flags |= C.GDAL_OF_UPDATE
}

type openSharedOpt struct{}

//Shared opens the dataset with OF_OPEN_SHARED
func Shared() interface {
	OpenOption
} {
	return openSharedOpt{}
}

func (openSharedOpt) setOpenOption(oo *openOptions) {
	oo.flags |= C.GDAL_OF_SHARED
}

type vectorOnlyOpt struct{}

//VectorOnly limits drivers to vector ones (incompatible with RasterOnly() )
func VectorOnly() interface {
	OpenOption
} {
	return vectorOnlyOpt{}
}
func (vectorOnlyOpt) setOpenOption(oo *openOptions) {
	oo.flags |= C.GDAL_OF_VECTOR
}

type rasterOnlyOpt struct{}

//RasterOnly limits drivers to vector ones (incompatible with VectorOnly() )
func RasterOnly() interface {
	OpenOption
} {
	return rasterOnlyOpt{}
}
func (rasterOnlyOpt) setOpenOption(oo *openOptions) {
	oo.flags |= C.GDAL_OF_RASTER
}

//SpatialRef is a wrapper around OGRSpatialReferenceH
type SpatialRef struct {
	handle  C.OGRSpatialReferenceH
	isOwned bool
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
	hndl := C.godalRasterize((*C.char)(cname), ds.handle(), cswitches.cPointer(), &errmsg, cconfig.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
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
	errmsg := C.godalRasterizeGeometry(ds.handle(), g.handle,
		cIntArray(opt.bands), C.int(len(opt.bands)), cDoubleArray(opt.values), C.int(opt.allTouched))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil

}

// GeometryType is a geometry type
type GeometryType uint32

const (
	//GTUnknown is a GeomtryType
	GTUnknown = GeometryType(C.wkbUnknown)
	//GTPoint is a GeomtryType
	GTPoint = GeometryType(C.wkbPoint)
	//GTLineString is a GeomtryType
	GTLineString = GeometryType(C.wkbLineString)
	//GTPolygon is a GeomtryType
	GTPolygon = GeometryType(C.wkbPolygon)
	//GTMultiPoint is a GeomtryType
	GTMultiPoint = GeometryType(C.wkbMultiPoint)
	//GTMultiLineString is a GeomtryType
	GTMultiLineString = GeometryType(C.wkbMultiLineString)
	//GTMultiPolygon is a GeomtryType
	GTMultiPolygon = GeometryType(C.wkbMultiPolygon)
	//GTGeometryCollection is a GeomtryType
	GTGeometryCollection = GeometryType(C.wkbGeometryCollection)
	//GTNone is a GeomtryType
	GTNone = GeometryType(C.wkbNone)
)

//FieldType is a vector field (attribute/column) type
type FieldType C.OGRFieldType

const (
	//FTInt is a Simple 32bit integer.
	FTInt = FieldType(C.OFTInteger)
	//FTReal is a Double Precision floating point.
	FTReal = FieldType(C.OFTReal)
	//FTString is a String of ASCII chars.
	FTString = FieldType(C.OFTString)
	//FTInt64 is a Single 64bit integer.
	FTInt64 = FieldType(C.OFTInteger64)
	//FTIntList is a List of 32bit integers.
	FTIntList = FieldType(C.OFTIntegerList)
	//FTRealList is a List of doubles.
	FTRealList = FieldType(C.OFTRealList)
	//FTStringList is a Array of strings.
	FTStringList = FieldType(C.OFTStringList)
	//FTBinary is a Raw Binary data.
	FTBinary = FieldType(C.OFTBinary)
	//FTDate is a Date.
	FTDate = FieldType(C.OFTDate)
	//FTTime is a Time.
	FTTime = FieldType(C.OFTTime)
	//FTDateTime is a Date and Time.
	FTDateTime = FieldType(C.OFTDateTime)
	//FTInt64List is a List of 64bit integers.
	FTInt64List = FieldType(C.OFTInteger64List)
)

//FieldDefinition defines a single attribute
type FieldDefinition struct {
	name  string
	ftype FieldType
}

//NewFieldDefinition creates a FieldDefinition
func NewFieldDefinition(name string, fdtype FieldType) *FieldDefinition {
	return &FieldDefinition{
		name:  name,
		ftype: fdtype,
	}
}

func (fd *FieldDefinition) setCreateLayerOpt(o *createLayerOpts) {
	o.fields = append(o.fields, fd)
}

func (fd *FieldDefinition) createHandle() C.OGRFieldDefnH {
	cfname := unsafe.Pointer(C.CString(fd.name))
	defer C.free(cfname)
	cfd := C.OGR_Fld_Create((*C.char)(cfname), C.OGRFieldType(fd.ftype))
	return cfd
}

// VectorTranslate runs the library version of ogr2ogr
// See the ogr2ogr doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//  []string{
//    "-f", "GeoJSON",
//	  "-t_srs","epsg:3857",
//    "-dstalpha"}
//
// Creation options and Driver may be set either in the switches slice with
//  switches:=[]string{"-dsco","TILED=YES", "-f","GeoJSON"}
// or through Options with
//  ds.VectorTranslate(dst, switches, CreationOption("TILED=YES","BLOCKXSIZE=256"), GeoJSON)
func (ds *Dataset) VectorTranslate(dstDS string, switches []string, opts ...DatasetVectorTranslateOption) (*Dataset, error) {
	gopts := dsVectorTranslateOpts{}
	for _, opt := range opts {
		opt.setDatasetVectorTranslateOpt(&gopts)
	}
	for _, copt := range gopts.creation {
		switches = append(switches, "-dsco", copt)
	}
	if gopts.driver != "" {
		dname := string(gopts.driver)
		if dm, ok := driverMappings[gopts.driver]; ok {
			dname = dm.vectorName
		}
		switches = append(switches, "-f", dname)
	}
	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()
	cconfig := sliceToCStringArray(gopts.config)
	defer cconfig.free()

	cname := unsafe.Pointer(C.CString(dstDS))
	defer C.free(cname)

	var errmsg *C.char
	hndl := C.godalDatasetVectorTranslate((*C.char)(cname), ds.handle(), cswitches.cPointer(), &errmsg, cconfig.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// Layer wraps an OGRLayerH
type Layer struct {
	majorObject
}

// handle returns a pointer to the underlying GDALRasterBandH
func (layer Layer) handle() C.OGRLayerH {
	return C.OGRLayerH(layer.majorObject.cHandle)
}

// FeatureCount returns the number of features in the layer
func (layer Layer) FeatureCount() (int, error) {
	var count C.int
	errmsg := C.godalLayerFeatureCount(layer.handle(), &count)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return 0, errors.New(C.GoString(errmsg))
	}
	return int(count), nil
}

// Layers returns all dataset layers
func (ds *Dataset) Layers() []Layer {
	clayers := C.godalVectorLayers(ds.handle())
	if clayers == nil {
		return nil
	}
	defer C.free(unsafe.Pointer(clayers))
	//https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
	sLayers := (*[1 << 30]C.OGRLayerH)(unsafe.Pointer(clayers))
	layers := []Layer{}
	i := 0
	for {
		if sLayers[i] == nil {
			return layers
		}
		layers = append(layers, Layer{majorObject{C.GDALMajorObjectH(sLayers[i])}})
		i++
	}
}

// SpatialRef returns dataset projection.
func (layer Layer) SpatialRef() *SpatialRef {
	hndl := C.OGR_L_GetSpatialRef(layer.handle())
	return &SpatialRef{handle: hndl, isOwned: false}
}

// Geometry wraps a OGRGeometryH
type Geometry struct {
	isOwned bool
	handle  C.OGRGeometryH
}

//Simplify simplifies the geometry with the given tolerance
func (g *Geometry) Simplify(tolerance float64) (*Geometry, error) {
	var errmsg *C.char
	hndl := C.godal_OGR_G_Simplify(g.handle, C.double(tolerance), &errmsg)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}, nil
}

//Buffer buffers the geometry
func (g *Geometry) Buffer(distance float64, segments int) (*Geometry, error) {
	var errmsg *C.char
	hndl := C.godal_OGR_G_Buffer(g.handle, C.double(distance), C.int(segments), &errmsg)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}, nil
}

//Empty retruens wether the underlying geometry is empty
func (g *Geometry) Empty() bool {
	e := C.OGR_G_IsEmpty(g.handle)
	return e != 0
}

//Bounds returns the geometry's envelope in the order minx,miny,maxx,maxy
func (g *Geometry) Bounds(opts ...BoundsOption) ([4]float64, error) {
	bo := boundsOpt{}
	for _, o := range opts {
		o.setBoundsOpt(&bo)
	}
	var env C.OGREnvelope
	C.OGR_G_GetEnvelope(g.handle, &env)
	bnds := [4]float64{
		float64(env.MinX),
		float64(env.MinY),
		float64(env.MaxX),
		float64(env.MaxY),
	}
	if bo.sr == nil {
		return bnds, nil
	}
	sr := g.SpatialRef()
	defer sr.Close()
	ret, err := reprojectBounds(bnds, sr, bo.sr)
	if err != nil {
		return bnds, err
	}
	return ret, nil
}

// Close may reclaim memory from geometry. Must be called exactly once.
func (g *Geometry) Close() {
	if g.handle == nil {
		return
		//panic("geometry already closed")
	}
	if g.isOwned {
		C.OGR_G_DestroyGeometry(g.handle)
	}
	g.handle = nil
}

//Feature is a Layer feature
type Feature struct {
	handle C.OGRFeatureH
}

//Geometry returns a handle to the feature's geometry
func (f *Feature) Geometry() *Geometry {
	hndl := C.OGR_F_GetGeometryRef(f.handle)
	return &Geometry{
		isOwned: false,
		handle:  hndl,
	}
}

//SetGeometry overwrites the feature's geometry
func (f *Feature) SetGeometry(geom *Geometry) error {
	errmsg := C.godalFeatureSetGeometry(f.handle, geom.handle)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

//Field is a Feature attribute
type Field struct {
	ftype    FieldType
	intVal   int64
	floatVal float64
	strVal   string
	//byteVal  []byte
}

//Type returns the field's native type
func (fld Field) Type() FieldType {
	return fld.ftype
}

//Int returns the Field as an integer
func (fld Field) Int() int64 {
	switch fld.ftype {
	case FTInt, FTInt64:
		return fld.intVal
	case FTReal:
		return int64(fld.floatVal)
	case FTString:
		ii, _ := strconv.Atoi(fld.strVal)
		return int64(ii)
	default:
		return 0
	}
}

//Float returns the field as a float64
func (fld Field) Float() float64 {
	switch fld.ftype {
	case FTInt, FTInt64:
		return float64(fld.intVal)
	case FTReal:
		return fld.floatVal
	case FTString:
		ii, _ := strconv.ParseFloat(fld.strVal, 64)
		return ii
	default:
		return 0
	}
}

//String returns the field as a string
func (fld Field) String() string {
	switch fld.ftype {
	case FTInt, FTInt64:
		return fmt.Sprintf("%d", fld.intVal)
	case FTReal:
		return fmt.Sprintf("%f", fld.floatVal)
	case FTString:
		return fld.strVal
	default:
		return ""
	}
}

//Fields returns all the Feature's fields
func (f *Feature) Fields() map[string]Field {
	fcount := C.OGR_F_GetFieldCount(f.handle)
	if fcount == 0 {
		return nil
	}
	retm := make(map[string]Field)
	for fid := C.int(0); fid < fcount; fid++ {
		fdefn := C.OGR_F_GetFieldDefnRef(f.handle, fid)
		fname := C.GoString(C.OGR_Fld_GetNameRef(fdefn))
		ftype := C.OGR_Fld_GetType(fdefn)
		fld := Field{}
		switch ftype {
		case C.OFTInteger:
			fld.ftype = FTInt
			fld.intVal = int64(C.OGR_F_GetFieldAsInteger64(f.handle, fid))
		case C.OFTInteger64:
			fld.ftype = FTInt64
			fld.intVal = int64(C.OGR_F_GetFieldAsInteger64(f.handle, fid))
		case C.OFTReal:
			fld.ftype = FTReal
			fld.floatVal = float64(C.OGR_F_GetFieldAsDouble(f.handle, fid))
		case C.OFTString:
			fld.ftype = FTString
			fld.strVal = C.GoString(C.OGR_F_GetFieldAsString(f.handle, fid))
			/*
				case C.OFTBinary:
					fallthrough
					//fld.ftype = FTBinary
					//var ll C.int
					//cdata := C.OGR_F_GetFieldAsBinary(f.handle, fid, &ll)
					//fld.byteVal = C.GoBytes(unsafe.Pointer(cdata), ll)
				case C.OFTDate:
					fallthrough
				case C.OFTTime:
					fallthrough
				case C.OFTDateTime:
					fallthrough
				case C.OFTInteger64List:
					fallthrough
				case C.OFTIntegerList:
					fallthrough
				case C.OFTStringList:
					fallthrough
				case C.OFTRealList:
					fallthrough
			*/
		default:
			//TODO
			continue
		}
		retm[fname] = fld
	}
	return retm
}

//Close releases resources associated to a feature
func (f *Feature) Close() {
	if f.handle == nil {
		return
		//panic("feature closed more than once")
	}
	C.OGR_F_Destroy(f.handle)
	f.handle = nil
}

// ResetReading makes Layer.NextFeature return the first feature of the layer
func (layer Layer) ResetReading() {
	C.OGR_L_ResetReading(layer.handle())
}

// NextFeature returns the layer's next feature, or nil if there are no mo features
func (layer Layer) NextFeature() *Feature {
	hndl := C.OGR_L_GetNextFeature(layer.handle())
	if hndl == nil {
		return nil
	}
	return &Feature{hndl}
}

// NewFeature creates a feature on Layer
func (layer Layer) NewFeature(geom *Geometry, opts ...NewFeatureOption) (*Feature, error) {
	nfo := newFeatureOpt{}
	for _, opt := range opts {
		opt.setNewFeatureOpt(&nfo)
	}
	var errmsg *C.char
	ghandle := C.OGRGeometryH(nil)
	if geom != nil {
		ghandle = geom.handle
	}
	hndl := C.godalLayerNewFeature(layer.handle(), ghandle, (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Feature{hndl}, nil
}

// UpdateFeature rewrites an updated feature in the Layer
func (layer Layer) UpdateFeature(feat *Feature) error {
	errmsg := C.godalLayerSetFeature(layer.handle(), feat.handle)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// DeleteFeature deletes feature from the Layer.
func (layer Layer) DeleteFeature(feat *Feature) error {
	errmsg := C.godalLayerDeleteFeature(layer.handle(), feat.handle)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// CreateLayer creates a new vector layer
//
// Available CreateLayerOptions are
//
// • FieldDefinition (may be used multiple times) to add attribute fields to the layer
func (ds *Dataset) CreateLayer(name string, sr *SpatialRef, gtype GeometryType, opts ...CreateLayerOption) (Layer, error) {
	co := createLayerOpts{}
	for _, opt := range opts {
		opt.setCreateLayerOpt(&co)
	}
	srHandle := C.OGRSpatialReferenceH(nil)
	if sr != nil {
		srHandle = sr.handle
	}
	var errmsg *C.char
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	hndl := C.godalCreateLayer(ds.handle(), (*C.char)(unsafe.Pointer(cname)), srHandle, C.OGRwkbGeometryType(gtype), (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return Layer{}, errors.New(C.GoString(errmsg))
	}
	if len(co.fields) > 0 {
		for _, fld := range co.fields {
			fhndl := fld.createHandle()
			//TODO error checking
			C.OGR_L_CreateField(hndl, fhndl, C.int(0))
			C.OGR_Fld_Destroy(fhndl)
		}
	}
	return Layer{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// NewGeometryFromWKT creates a new Geometry from its WKT representation
func NewGeometryFromWKT(wkt string, sr *SpatialRef) (*Geometry, error) {
	srHandle := C.OGRSpatialReferenceH(nil)
	if sr != nil {
		srHandle = sr.handle
	}
	var errmsg *C.char
	cwkt := C.CString(wkt)
	defer C.free(unsafe.Pointer(cwkt))
	hndl := C.godalNewGeometryFromWKT((*C.char)(unsafe.Pointer(cwkt)), srHandle, (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Geometry{isOwned: true, handle: hndl}, nil
}

// NewGeometryFromWKB creates a new Geometry from its WKB representation
func NewGeometryFromWKB(wkb []byte, sr *SpatialRef) (*Geometry, error) {
	srHandle := C.OGRSpatialReferenceH(nil)
	if sr != nil {
		srHandle = sr.handle
	}
	var errmsg *C.char
	hndl := C.godalNewGeometryFromWKB(unsafe.Pointer(&wkb[0]), C.int(len(wkb)), srHandle, (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Geometry{isOwned: true, handle: hndl}, nil
}

//WKT returns the Geomtry's WKT representation
func (g *Geometry) WKT() (string, error) {
	var errmsg *C.char
	cwkt := C.godalExportGeometryWKT(g.handle, (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return "", errors.New(C.GoString(errmsg))
	}
	wkt := C.GoString(cwkt)
	C.CPLFree(unsafe.Pointer(cwkt))
	return wkt, nil
}

//WKB returns the Geomtry's WKB representation
func (g *Geometry) WKB() ([]byte, error) {
	var cwkb unsafe.Pointer
	clen := C.int(0)
	errmsg := C.godalExportGeometryWKB(&cwkb, &clen, g.handle)
	/* wkb export never errors
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	*/
	_ = errmsg
	wkb := C.GoBytes(unsafe.Pointer(cwkb), clen)
	C.free(unsafe.Pointer(cwkb))
	return wkb, nil
}

// SpatialRef returns the geometry's SpatialRef
func (g *Geometry) SpatialRef() *SpatialRef {
	hndl := C.OGR_G_GetSpatialReference(g.handle)
	return &SpatialRef{
		handle:  hndl,
		isOwned: false,
	}
}

// SetSpatialRef assigns the given SpatialRef to the Geometry. It does not
// perform an actual reprojection.
func (g *Geometry) SetSpatialRef(sr *SpatialRef) {
	C.OGR_G_AssignSpatialReference(g.handle, sr.handle)
}

// Reproject reprojects the given geometry to the given SpatialRef
func (g *Geometry) Reproject(to *SpatialRef) error {
	errmsg := C.godalGeometryTransformTo(g.handle, to.handle)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// Transform transforms the given geometry. g is expected to already be
// in the supplied Transform source SpatialRef.
func (g *Geometry) Transform(trn *Transform) error {
	errmsg := C.godalGeometryTransform(g.handle, trn.handle, trn.dst)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// GeoJSON returns the geometry in geojson format. The geometry is expected to be in epsg:4326
// projection per RFCxxx
//
// Available GeoJSONOptions are
//
// • SignificantDigits(n int) to keep n significant digits after the decimal separator (default: 8)
func (g *Geometry) GeoJSON(opts ...GeoJSONOption) (string, error) {
	gjo := geojsonOpt{
		precision: 7,
	}
	for _, opt := range opts {
		opt.setGeojsonOpt(&gjo)
	}
	var errmsg *C.char
	gjdata := C.godalExportGeometryGeoJSON(g.handle, C.int(gjo.precision), (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return "", errors.New(C.GoString(errmsg))
	}
	wkt := C.GoString(gjdata)
	C.CPLFree(unsafe.Pointer(gjdata))
	return wkt, nil

}

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
	Size() int64
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
	return C.longlong(cbd.Size())
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
					if err != nil {
						*errorString = C.CString(err.Error())
					} else {
						*errorString = C.CString("short read")
					}
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

type osioAdapterWrapper struct {
	*osio.Adapter
}

func (ga osioAdapterWrapper) VSIReader(key string) (VSIReader, error) {
	return ga.Reader(key)
}

func RegisterVSIAdapter(prefix string, keyReader *osio.Adapter, opts ...VSIHandlerOption) error {
	return RegisterVSIHandler(prefix, osioAdapterWrapper{keyReader}, opts...)
}

// RegisterVSIHandler registers keyReader on the given prefix.
// When registering a reader with
//  RegisterVSIHandler("scheme://",handler)
// calling Open("scheme://myfile.txt") will result in godal making calls to
//  VSIKeyReader("myfile.txt").ReadAt(buf,offset)
func RegisterVSIHandler(prefix string, keyReader VSIKeyReader, opts ...VSIHandlerOption) error {
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
		return fmt.Errorf("handler already registered on prefix")
	}
	errmsg := C.VSIInstallGoHandler(C.CString(prefix), opt.bufferSize, opt.cacheSize)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	handlers[prefix] = keyReader
	return nil
}

//BuildVRT runs the GDALBuildVRT function and creates a VRT dataset from a list of datasets
func BuildVRT(dstVRTName string, sourceDatasets []string, switches []string, opts ...BuildVRTOption) (*Dataset, error) {
	bvo := buildVRTOpts{}
	for _, o := range opts {
		o.setBuildVRTOpt(&bvo)
	}
	if bvo.resampling != Nearest {
		switches = append(switches, "-r", bvo.resampling.String())
	}
	for _, b := range bvo.bands {
		switches = append(switches, "-b", fmt.Sprintf("%d", b))
	}
	for _, oo := range bvo.openOptions {
		switches = append(switches, "-oo", oo)
	}
	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()
	cconfig := sliceToCStringArray(bvo.config)
	defer cconfig.free()

	cname := unsafe.Pointer(C.CString(dstVRTName))
	defer C.free(cname)

	csources := sliceToCStringArray(sourceDatasets)
	defer csources.free()

	var errmsg *C.char
	hndl := C.godalBuildVRT((*C.char)(cname), csources.cPointer(),
		cswitches.cPointer(), &errmsg, cconfig.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

type cgoContext struct {
	cctx *C.cctx
	opts cStringArray
}

func createCGOContext(configOptions []string, eh ErrorHandler) cgoContext {
	cgc := cgoContext{
		opts: sliceToCStringArray(configOptions),
		cctx: (*C.cctx)(C.malloc(C.size_t(unsafe.Sizeof(C.cctx{})))),
	}
	cgc.cctx.configOptions = cgc.opts.cPointer()
	cgc.cctx.failed = 0
	cgc.cctx.errMessage = nil
	if eh != nil {
		cgc.cctx.handlerIdx = C.int(registerErrorHandler(eh))
	} else {
		cgc.cctx.handlerIdx = 0
	}
	return cgc
}

func (cgc cgoContext) cPointer() *C.cctx {
	return cgc.cctx
}

//frees the context and returns any error it may contain
func (cgc cgoContext) close() error {
	cgc.opts.free()
	if cgc.cctx.errMessage != nil {
		if cgc.cctx.handlerIdx != 0 {
			panic("bug!")
		}
		defer C.free(unsafe.Pointer(cgc.cctx.errMessage))
		return errors.New(C.GoString(cgc.cctx.errMessage))
	}
	if cgc.cctx.handlerIdx != 0 {
		defer unregisterErrorHandler(int(cgc.cctx.handlerIdx))
		errs := getErrorHandler(int(cgc.cctx.handlerIdx)).errors
		if errs != nil {
			if len(errs) == 1 {
				return errs[0]
			} else {
				msgs := []string{errs[0].Error()}
				for i := 1; i < len(errs); i++ {
					msgs = append(msgs, errs[i].Error())
				}
				return errors.New(strings.Join(msgs, "\n"))
			}
		}
	}
	return nil
}
