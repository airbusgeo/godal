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
#cgo CXXFLAGS: -std=c++11
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
	"time"
	"unsafe"
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
	//Int8 DataType (GDAL>=3.7.0)
	// [RFC 87]: https://gdal.org/development/rfc/rfc87_signed_int8.html
	Int8 = DataType(C.GDT_Int8)
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

// ErrorCategory wraps GDAL's error types
type ErrorCategory int

const (
	// CE_None is not an error
	CE_None = ErrorCategory(C.CE_None)
	// CE_Debug is a debug level
	CE_Debug = ErrorCategory(C.CE_Debug)
	// CE_Warning is a warning levele
	CE_Warning = ErrorCategory(C.CE_Warning)
	// CE_Failure is an error
	CE_Failure = ErrorCategory(C.CE_Failure)
	// CE_Fatal is an unrecoverable error
	CE_Fatal = ErrorCategory(C.CE_Fatal)
)

// String implements Stringer
func (dtype DataType) String() string {
	return C.GoString(C.GDALGetDataTypeName(C.GDALDataType(dtype)))
}

// Size retruns the number of bytes needed for one instance of DataType
func (dtype DataType) Size() int {
	switch dtype {
	case Byte, Int8:
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

// ColorInterp is a band's color interpretation
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
	var scale, offset C.double
	C.godalBandStructure(band.handle(), &sx, &sy, &bsx, &bsy, &scale, &offset, &dtype)
	return BandStructure{
		SizeX:      int(sx),
		SizeY:      int(sy),
		BlockSizeX: int(bsx),
		BlockSizeY: int(bsy),
		Scale:      float64(scale),
		Offset:     float64(offset),
		DataType:   DataType(int(dtype)),
	}
}

// NoData returns the band's nodata value. if ok is false, the band does not
// have a nodata value set
func (band Band) NoData() (nodata float64, ok bool) {
	cok := C.int(0)
	cn := C.GDALGetRasterNoDataValue(band.handle(), &cok)
	if cok != 0 {
		return float64(cn), true
	}
	return 0, false
}

// SetNoData sets the band's nodata value
func (band Band) SetNoData(nd float64, opts ...SetNoDataOption) error {
	sndo := &setNodataOpts{}
	for _, opt := range opts {
		opt.setSetNoDataOpt(sndo)
	}
	cgc := createCGOContext(nil, sndo.errorHandler)
	C.godalSetRasterNoDataValue(cgc.cPointer(), band.handle(), C.double(nd))
	return cgc.close()
}

// ClearNoData clears the band's nodata value
func (band Band) ClearNoData(opts ...SetNoDataOption) error {
	sndo := &setNodataOpts{}
	for _, opt := range opts {
		opt.setSetNoDataOpt(sndo)
	}
	cgc := createCGOContext(nil, sndo.errorHandler)
	C.godalDeleteRasterNoDataValue(cgc.cPointer(), band.handle())
	return cgc.close()
}

// SetScaleOffset sets the band's scale and offset
func (band Band) SetScaleOffset(scale, offset float64, opts ...SetScaleOffsetOption) error {
	setterOpts := &setScaleOffsetOpts{}
	for _, opt := range opts {
		opt.setSetScaleOffsetOpt(setterOpts)
	}
	cgc := createCGOContext(nil, setterOpts.errorHandler)
	C.godalSetRasterScaleOffset(cgc.cPointer(), band.handle(), C.double(scale), C.double(offset))
	return cgc.close()
}

// ClearScaleOffset clears the band's scale and offset
func (band Band) ClearScaleOffset(opts ...SetScaleOffsetOption) error {
	return band.SetScaleOffset(1.0, 0.0, opts...)
}

// ColorInterp returns the band's color interpretation (defaults to Gray)
func (band Band) ColorInterp() ColorInterp {
	colorInterp := C.GDALGetRasterColorInterpretation(band.handle())
	return ColorInterp(colorInterp)
}

// SetColorInterp sets the band's color interpretation
func (band Band) SetColorInterp(colorInterp ColorInterp, opts ...SetColorInterpOption) error {
	scio := &setColorInterpOpts{}
	for _, opt := range opts {
		opt.setSetColorInterpOpt(scio)
	}

	cgc := createCGOContext(nil, scio.errorHandler)
	C.godalSetRasterColorInterpretation(cgc.cPointer(), band.handle(), C.GDALColorInterp(colorInterp))
	return cgc.close()
}

// MaskFlags returns the mask flags associated with this band.
//
// See https://gdal.org/development/rfc/rfc15_nodatabitmask.html for how this flag
// should be interpreted
func (band Band) MaskFlags() int {
	return int(C.GDALGetMaskFlags(band.handle()))
}

// MaskBand returns the mask (nodata) band for this band. May be generated from nodata values.
func (band Band) MaskBand() Band {
	hndl := C.GDALGetMaskBand(band.handle())
	return Band{majorObject{C.GDALMajorObjectH(hndl)}}
}

// CreateMask creates a mask (nodata) band for this band.
//
// Any handle returned by a previous call to MaskBand() should not be used after a call to CreateMask
// See https://gdal.org/development/rfc/rfc15_nodatabitmask.html for how flag should be used
func (band Band) CreateMask(flags int, opts ...BandCreateMaskOption) (Band, error) {
	gopts := bandCreateMaskOpts{}
	for _, opt := range opts {
		opt.setBandCreateMaskOpt(&gopts)
	}
	cgc := createCGOContext(gopts.config, gopts.errorHandler)
	hndl := C.godalCreateMaskBand(cgc.cPointer(), band.handle(), C.int(flags))
	if err := cgc.close(); err != nil {
		return Band{}, err
	}
	return Band{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// Fill sets the whole band uniformely to (real,imag)
func (band Band) Fill(real, imag float64, opts ...FillBandOption) error {
	fo := &fillBandOpts{}
	for _, o := range opts {
		o.setFillBandOpt(fo)
	}
	cgc := createCGOContext(nil, fo.errorHandler)
	C.godalFillRaster(cgc.cPointer(), band.handle(), C.double(real), C.double(imag))
	return cgc.close()
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
	ro := bandIOOpts{}
	for _, opt := range opts {
		opt.setBandIOOpt(&ro)
	}
	if ro.dsHeight == 0 {
		ro.dsHeight = bufHeight
	}
	if ro.dsWidth == 0 {
		ro.dsWidth = bufWidth
	}
	dtype := bufferType(buffer)
	dsize := dtype.Size()

	pixelSpacing := dsize
	if ro.pixelSpacing > 0 {
		pixelSpacing = ro.pixelSpacing
	}
	if ro.pixelStride > 0 {
		pixelSpacing = ro.pixelStride * dsize
	}
	lineSpacing := bufWidth * pixelSpacing
	if ro.lineSpacing > 0 {
		lineSpacing = ro.lineSpacing
	}
	if ro.lineStride > 0 {
		lineSpacing = ro.lineStride * dsize
	}

	minsize := (lineSpacing*(bufHeight-1) + (bufWidth-1)*pixelSpacing + dsize) / dsize
	cBuf := cBuffer(buffer, minsize)
	//fmt.Fprintf(os.Stderr, "%v %d %d %d\n", ro.bands, pixelSpacing, lineSpacing, bandSpacing)
	ralg, err := ro.resampling.rioAlg()
	if err != nil {
		return err
	}
	cgc := createCGOContext(ro.config, ro.errorHandler)
	C.godalBandRasterIO(cgc.cPointer(), band.handle(), C.GDALRWFlag(rw),
		C.int(srcX), C.int(srcY), C.int(ro.dsWidth), C.int(ro.dsHeight),
		cBuf,
		C.int(bufWidth), C.int(bufHeight), C.GDALDataType(dtype),
		C.int(pixelSpacing), C.int(lineSpacing), ralg)
	return cgc.close()
}

// Polygonize wraps GDALPolygonize
func (band Band) Polygonize(dstLayer Layer, opts ...PolygonizeOption) error {
	popt := polygonizeOpts{
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

	cgc := createCGOContext(nil, popt.errorHandler)
	C.godalPolygonize(cgc.cPointer(), band.handle(), cMaskBand, dstLayer.handle(), C.int(popt.pixFieldIndex), copts.cPointer())
	return cgc.close()
}

// FillNoData wraps GDALFillNodata()
func (band Band) FillNoData(opts ...FillNoDataOption) error {
	popt := fillnodataOpts{
		maxDistance: 100,
		iterations:  0,
	}

	for _, opt := range opts {
		opt.setFillnodataOpt(&popt)
	}
	//copts := sliceToCStringArray(popt.options)
	//defer copts.free()
	var cMaskBand C.GDALRasterBandH = nil
	if popt.mask != nil {
		cMaskBand = popt.mask.handle()
	}

	cgc := createCGOContext(nil, popt.errorHandler)
	C.godalFillNoData(cgc.cPointer(), band.handle(), cMaskBand, C.int(popt.maxDistance), C.int(popt.iterations), nil)
	return cgc.close()
}

// SieveFilter wraps GDALSieveFilter
func (band Band) SieveFilter(sizeThreshold int, opts ...SieveFilterOption) error {
	sfopt := sieveFilterOpts{
		dstBand:       &band,
		connectedness: 4,
	}
	maskBand := band.MaskBand()
	sfopt.mask = &maskBand

	for _, opt := range opts {
		opt.setSieveFilterOpt(&sfopt)
	}
	var cMaskBand C.GDALRasterBandH = nil
	if sfopt.mask != nil {
		cMaskBand = sfopt.mask.handle()
	}
	cgc := createCGOContext(nil, sfopt.errorHandler)
	C.godalSieveFilter(cgc.cPointer(), band.handle(), cMaskBand, sfopt.dstBand.handle(),
		C.int(sizeThreshold), C.int(sfopt.connectedness))
	return cgc.close()
}

// Overviews returns all overviews of band
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

// Histogram returns or computes the bands histogram
func (band Band) Histogram(opts ...HistogramOption) (Histogram, error) {
	hopt := histogramOpts{}
	for _, o := range opts {
		o.setHistogramOpt(&hopt)
	}
	var values *C.ulonglong = nil
	defer C.VSIFree(unsafe.Pointer(values))

	cgc := createCGOContext(nil, hopt.errorHandler)

	C.godalRasterHistogram(cgc.cPointer(), band.handle(), (*C.double)(&hopt.min), (*C.double)(&hopt.max), (*C.int)(&hopt.buckets),
		&values, C.int(hopt.includeOutside), C.int(hopt.approx))
	if err := cgc.close(); err != nil {
		return Histogram{}, err
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

// GetStatistics returns if present and flag as true.
//
// Only cached statistics are returned and no new statistics are computed.
// Return false and no error if no statistics are availables.
// Available options are:
// - Aproximate() to allow the satistics to be computed on overviews or a subset of all tiles.
// - ErrLogger
func (band Band) GetStatistics(opts ...StatisticsOption) (Statistics, bool, error) {
	sopt := statisticsOpts{}
	for _, s := range opts {
		s.setStatisticsOpt(&sopt)
	}
	var min, max, mean, std C.double
	cgc := createCGOContext(nil, sopt.errorHandler)
	ret := C.godalGetRasterStatistics(cgc.cPointer(), band.handle(),
		(C.int)(sopt.approx), &min, &max, &mean, &std)
	if err := cgc.close(); err != nil {
		return Statistics{}, false, err
	}
	if ret == 0 {
		return Statistics{}, false, nil
	}
	var ap bool = sopt.approx != 0
	s := Statistics{
		Approximate: ap,
		Min:         float64(min),
		Max:         float64(max),
		Mean:        float64(mean),
		Std:         float64(std),
	}
	return s, true, nil
}

// ComputeStatistics returns from exact computation or approximation.
//
// Band full scan might be necessary.
// Available options are:
// - Aproximate() to allow the satistics to be computed on overviews or a subset of all tiles.
// - ErrLogger
func (band Band) ComputeStatistics(opts ...StatisticsOption) (Statistics, error) {
	sopt := statisticsOpts{}
	for _, s := range opts {
		s.setStatisticsOpt(&sopt)
	}
	var min, max, mean, std C.double
	cgc := createCGOContext(nil, sopt.errorHandler)
	C.godalComputeRasterStatistics(cgc.cPointer(), band.handle(),
		(C.int)(sopt.approx), &min, &max, &mean, &std)
	if err := cgc.close(); err != nil {
		return Statistics{}, err
	}
	var ap bool = sopt.approx != 0
	s := Statistics{
		Min:         float64(min),
		Max:         float64(max),
		Mean:        float64(mean),
		Std:         float64(std),
		Approximate: ap,
	}
	return s, nil
}

// SetStatistics set statistics (Min, Max, Mean & STD).
//
// Available options are:
//
//	-ErrLogger
func (band Band) SetStatistics(min, max, mean, std float64, opts ...SetStatisticsOption) error {
	stso := setStatisticsOpt{}
	for _, opt := range opts {
		opt.setSetStatisticsOpt(&stso)
	}
	cgc := createCGOContext(nil, stso.errorHandler)
	C.godalSetRasterStatistics(cgc.cPointer(), band.handle(), C.double(min),
		C.double(max), C.double(mean), C.double(std))
	if err := cgc.close(); err != nil {
		return err
	}
	return nil
}

func cIntArray(in []int) *C.int {
	var ptr *C.int
	if len(in) > 0 {
		ret := make([]C.int, len(in))
		for i := range in {
			ret[i] = C.int(in[i])
		}
		ptr = (*C.int)(unsafe.Pointer(&ret[0]))
	}
	return ptr
}

func cLongArray(in []int64) *C.longlong {
	var ptr *C.longlong
	if len(in) > 0 {
		ret := make([]C.longlong, len(in))
		for i := range in {
			ret[i] = C.longlong(in[i])
		}
		ptr = (*C.longlong)(unsafe.Pointer(&ret[0]))
	}
	return ptr
}

func cDoubleArray(in []float64) *C.double {
	var ptr *C.double
	if len(in) > 0 {
		ret := make([]C.double, len(in))
		for i := range in {
			ret[i] = C.double(in[i])
		}
		ptr = (*C.double)(unsafe.Pointer(&ret[0]))
	}
	return ptr
}

type cStringArray struct {
	arr **C.char
	l   int
}

func (ca cStringArray) free() {
	if ca.l > 0 {
		garr := (*[1 << 30]*C.char)(unsafe.Pointer(ca.arr))[0:ca.l:ca.l]
		for i := 0; i < ca.l-1; i++ {
			C.free(unsafe.Pointer(garr[i]))
		}
		C.free(unsafe.Pointer(ca.arr))
	}
}

func (ca cStringArray) cPointer() **C.char {
	return ca.arr
}

func sliceToCStringArray(in []string) cStringArray {
	if len(in) > 0 {
		csa := cStringArray{l: len(in) + 1}
		csa.arr = (**C.char)(C.malloc(C.size_t(csa.l) * C.size_t(unsafe.Sizeof((*C.char)(nil)))))
		garr := (*[1 << 30]*C.char)(unsafe.Pointer(csa.arr))[0:csa.l:csa.l]
		for i := range in {
			garr[i] = C.CString(in[i])
		}
		garr[len(in)] = nil
		return csa
	}
	return cStringArray{}
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

func cIntArrayToSlice(in *C.int, length C.int) []int64 {
	if in == nil {
		return nil
	}
	cSlice := (*[1 << 28]C.int)(unsafe.Pointer(in))[:length:length]
	slice := make([]int64, length)
	for i, cval := range cSlice {
		slice[i] = int64(cval)
	}
	return slice
}

func cLongArrayToSlice(in *C.longlong, length C.int) []int64 {
	if in == nil {
		return nil
	}
	cSlice := (*[1 << 28]C.longlong)(unsafe.Pointer(in))[:length:length]
	slice := make([]int64, length)
	for i, cval := range cSlice {
		slice[i] = int64(cval)
	}
	return slice
}

func cDoubleArrayToSlice(in *C.double, length C.int) []float64 {
	if in == nil {
		return nil
	}
	cSlice := (*[1 << 28]C.double)(unsafe.Pointer(in))[:length:length]
	slice := make([]float64, length)
	for i, cval := range cSlice {
		slice[i] = float64(cval)
	}
	return slice
}

// PaletteInterp defines the color interpretation of a ColorTable
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

// ColorTable is a color table associated with a Band
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

// ColorTable returns the bands color table. The returned ColorTable will have
// a 0-length Entries if the band has no color table assigned
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
func (band Band) SetColorTable(ct ColorTable, opts ...SetColorTableOption) error {
	cto := &setColorTableOpts{}
	for _, o := range opts {
		o.setSetColorTableOpt(cto)
	}
	var cshorts *C.short
	if len(ct.Entries) > 0 {
		cshorts = cColorTableArray(ct.Entries)
	}
	cgc := createCGOContext(nil, cto.errorHandler)
	C.godalSetColorTable(cgc.cPointer(), band.handle(), C.GDALPaletteInterp(ct.PaletteInterp), C.int(len(ct.Entries)), cshorts)
	return cgc.close()
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
//
//	[MinX, MinY, MaxX, MaxY]
func (ds *Dataset) Bounds(opts ...BoundsOption) ([4]float64, error) {

	bo := boundsOpts{}
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

// CreateMaskBand creates a mask (nodata) band shared for all bands of this dataset.
//
// Any handle returned by a previous call to Band.MaskBand() should not be used after a call to CreateMaskBand
// See https://gdal.org/development/rfc/rfc15_nodatabitmask.html for how flag should be used
func (ds *Dataset) CreateMaskBand(flags int, opts ...DatasetCreateMaskOption) (Band, error) {
	gopts := dsCreateMaskOpts{}
	for _, opt := range opts {
		opt.setDatasetCreateMaskOpt(&gopts)
	}
	cgc := createCGOContext(gopts.config, gopts.errorHandler)
	hndl := C.godalCreateDatasetMaskBand(cgc.cPointer(), ds.handle(), C.int(flags))
	if err := cgc.close(); err != nil {
		return Band{}, err
	}
	return Band{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// Driver returns dataset driver.
func (ds *Dataset) Driver() Driver {
	return Driver{majorObject{C.GDALMajorObjectH(C.GDALGetDatasetDriver(ds.handle()))}}
}

// Projection returns the WKT projection of the dataset. May be empty.
func (ds *Dataset) Projection() string {
	str := C.GDALGetProjectionRef(ds.handle())
	return C.GoString(str)
}

// SetProjection sets the WKT projection of the dataset. May be empty.
func (ds *Dataset) SetProjection(wkt string, opts ...SetProjectionOption) error {
	po := &setProjectionOpts{}
	for _, o := range opts {
		o.setSetProjectionOpt(po)
	}
	var cwkt = (*C.char)(nil)
	if len(wkt) > 0 {
		cwkt = C.CString(wkt)
		defer C.free(unsafe.Pointer(cwkt))
	}
	cgc := createCGOContext(nil, po.errorHandler)
	C.godalSetProjection(cgc.cPointer(), ds.handle(), cwkt)
	return cgc.close()
}

// SpatialRef returns dataset projection.
func (ds *Dataset) SpatialRef() *SpatialRef {
	hndl := C.GDALGetSpatialRef(ds.handle())
	return &SpatialRef{handle: hndl, isOwned: false}
}

// SetSpatialRef sets dataset's projection.
//
// sr can be set to nil to clear an existing projection
func (ds *Dataset) SetSpatialRef(sr *SpatialRef, opts ...SetSpatialRefOption) error {
	sro := &setSpatialRefOpts{}
	for _, o := range opts {
		o.setSetSpatialRefOpt(sro)
	}
	var hndl C.OGRSpatialReferenceH
	if sr == nil {
		hndl = nil
	} else {
		hndl = sr.handle
	}
	cgc := createCGOContext(nil, sro.errorHandler)
	C.godalDatasetSetSpatialRef(cgc.cPointer(), ds.handle(), hndl)
	return cgc.close()
}

// GeoTransform returns the affine transformation coefficients
func (ds *Dataset) GeoTransform(opts ...GetGeoTransformOption) ([6]float64, error) {
	gto := &getGeoTransformOpts{}
	for _, o := range opts {
		o.setGetGeoTransformOpt(gto)
	}
	ret := [6]float64{}
	gt := make([]C.double, 6)
	cgt := (*C.double)(unsafe.Pointer(&gt[0]))
	cgc := createCGOContext(nil, gto.errorHandler)
	C.godalGetGeoTransform(cgc.cPointer(), ds.handle(), cgt)
	if err := cgc.close(); err != nil {
		return ret, err
	}
	for i := range ret {
		ret[i] = float64(gt[i])
	}
	return ret, nil
}

// SetGeoTransform sets the affine transformation coefficients
func (ds *Dataset) SetGeoTransform(transform [6]float64, opts ...SetGeoTransformOption) error {
	gto := &setGeoTransformOpts{}
	for _, o := range opts {
		o.setSetGeoTransformOpt(gto)
	}
	gt := cDoubleArray(transform[:])
	cgc := createCGOContext(nil, gto.errorHandler)
	C.godalSetGeoTransform(cgc.cPointer(), ds.handle(), gt)
	return cgc.close()
}

// SetNoData sets the band's nodata value
func (ds *Dataset) SetNoData(nd float64, opts ...SetNoDataOption) error {
	sndo := &setNodataOpts{}
	for _, opt := range opts {
		opt.setSetNoDataOpt(sndo)
	}
	cgc := createCGOContext(nil, sndo.errorHandler)
	C.godalSetDatasetNoDataValue(cgc.cPointer(), ds.handle(), C.double(nd))
	return cgc.close()
}

// SetScaleOffset sets the band's scale and offset
func (ds *Dataset) SetScaleOffset(scale, offset float64, opts ...SetScaleOffsetOption) error {
	setterOpts := &setScaleOffsetOpts{}
	for _, opt := range opts {
		opt.setSetScaleOffsetOpt(setterOpts)
	}
	cgc := createCGOContext(nil, setterOpts.errorHandler)
	C.godalSetDatasetScaleOffset(cgc.cPointer(), ds.handle(), C.double(scale), C.double(offset))
	return cgc.close()
}

// Translate runs the library version of gdal_translate.
// See the gdal_translate doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//
//	[]string{
//	  "-a_nodata", 0,
//	  "-a_srs", "epsg:4326"}
//
// Creation options and driver may be set either in the switches slice with
//
//	switches:=[]string{"-co","TILED=YES","-of","GTiff"}
//
// or through Options with
//
//	ds.Translate(dst, switches, CreationOption("TILED=YES","BLOCKXSIZE=256"), GTiff)
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
	cname := unsafe.Pointer(C.CString(dstDS))
	defer C.free(cname)

	cgc := createCGOContext(gopts.config, gopts.errorHandler)
	hndl := C.godalTranslate(cgc.cPointer(), (*C.char)(cname), ds.handle(), cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// Warp runs the library version of gdalwarp
// See the gdalwarp doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//
//	 []string{
//		  "-t_srs","epsg:3857",
//	   "-dstalpha"}
//
// Creation options and driver may be set either in the switches slice with
//
//	switches:=[]string{"-co","TILED=YES","-of","GTiff"}
//
// or through Options with
//
//	ds.Warp(dst, switches, CreationOption("TILED=YES","BLOCKXSIZE=256"), GTiff)
func (ds *Dataset) Warp(dstDS string, switches []string, opts ...DatasetWarpOption) (*Dataset, error) {
	return Warp(dstDS, []*Dataset{ds}, switches, opts...)
}

// Warp writes provided sourceDS Datasets into new dataset and runs the library version of gdalwarp
// See the gdalwarp doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//
//	 []string{
//		  "-t_srs","epsg:3857",
//	   "-dstalpha"}
//
// Creation options and driver may be set either in the switches slice with
//
//	switches:=[]string{"-co","TILED=YES","-of","GTiff"}
//
// or through Options with
//
//	ds.Warp(dst, switches, CreationOption("TILED=YES","BLOCKXSIZE=256"), GTiff)
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
	cname := unsafe.Pointer(C.CString(dstDS))
	defer C.free(cname)

	cgc := createCGOContext(gopts.config, gopts.errorHandler)
	hndl := C.godalDatasetWarp(cgc.cPointer(), (*C.char)(cname), C.int(len(sourceDS)), (*C.GDALDatasetH)(unsafe.Pointer(&srcDS[0])), cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// WarpInto writes provided sourceDS Datasets into self existing dataset and runs the library version of gdalwarp
// See the gdalwarp doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//
//	 []string{
//		  "-t_srs","epsg:3857",
//	   "-dstalpha"}
func (ds *Dataset) WarpInto(sourceDS []*Dataset, switches []string, opts ...DatasetWarpIntoOption) error {
	gopts := dsWarpIntoOpts{}
	for _, opt := range opts {
		opt.setDatasetWarpIntoOpt(&gopts)
	}

	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()

	dstDS := ds.handle()
	srcDS := make([]C.GDALDatasetH, len(sourceDS))
	for i, dataset := range sourceDS {
		srcDS[i] = dataset.handle()
	}

	cgc := createCGOContext(gopts.config, gopts.errorHandler)
	C.godalDatasetWarpInto(cgc.cPointer(),
		dstDS,
		C.int(len(sourceDS)),
		(*C.GDALDatasetH)(unsafe.Pointer(&srcDS[0])),
		cswitches.cPointer())
	return cgc.close()
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
	cResample := unsafe.Pointer(C.CString(oopts.resampling.String()))
	defer C.free(cResample)

	cgc := createCGOContext(oopts.config, oopts.errorHandler)
	C.godalBuildOverviews(cgc.cPointer(), ds.handle(), (*C.char)(cResample), nLevels, cLevels,
		nBands, cBands)
	return cgc.close()
}

// ClearOverviews deletes all dataset overviews
func (ds *Dataset) ClearOverviews(opts ...ClearOverviewsOption) error {
	co := &clearOvrOpts{}
	for _, o := range opts {
		o.setClearOverviewsOpt(co)
	}
	cgc := createCGOContext(nil, co.errorHandler)
	C.godalClearOverviews(cgc.cPointer(), ds.handle())
	return cgc.close()
}

// ClearStatistics delete dataset statisitics
//
// Since GDAL 3.2
// Available options are:
//
//	-ErrLogger
func (ds *Dataset) ClearStatistics(opts ...ClearStatisticsOption) error {
	cls := &clearStatisticsOpt{}
	for _, o := range opts {
		o.setClearStatisticsOpt(cls)
	}
	cgc := createCGOContext(nil, cls.errorHandler)
	C.godalClearRasterStatistics(cgc.cPointer(), ds.handle())
	return cgc.close()
}

// Structure returns the dataset's Structure
func (ds *Dataset) Structure() DatasetStructure {
	var sx, sy, bsx, bsy, bandCount, dtype C.int
	var scale, offset C.double
	C.godalDatasetStructure(ds.handle(), &sx, &sy, &bsx, &bsy, &scale, &offset, &bandCount, &dtype)
	return DatasetStructure{
		BandStructure: BandStructure{
			SizeX:      int(sx),
			SizeY:      int(sy),
			BlockSizeX: int(bsx),
			BlockSizeY: int(bsy),
			Scale:      float64(scale),
			Offset:     float64(offset),
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
	ro := datasetIOOpts{}
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
	dtype := bufferType(buffer)
	dsize := dtype.Size()

	pixelSpacing := dsize * len(ro.bands)
	if ro.pixelSpacing > 0 {
		pixelSpacing = ro.pixelSpacing
	}
	if ro.pixelStride > 0 {
		pixelSpacing = ro.pixelStride * dsize
	}

	lineSpacing := bufWidth * pixelSpacing
	if ro.lineSpacing > 0 {
		lineSpacing = ro.lineSpacing
	}
	if ro.lineStride > 0 {
		lineSpacing = ro.lineStride * dsize
	}

	bandSpacing := dsize
	if ro.bandSpacing > 0 {
		bandSpacing = ro.bandSpacing
	}
	if ro.bandStride > 0 {
		bandSpacing = ro.bandStride * dsize
	}

	if ro.bandInterleave {
		pixelSpacing = dsize
		lineSpacing = bufWidth * dsize
		bandSpacing = bufHeight * bufWidth * dsize
	}

	minsize := ((len(ro.bands)-1)*bandSpacing + (bufHeight-1)*lineSpacing + (bufWidth-1)*pixelSpacing + dsize) / dsize
	cBuf := cBuffer(buffer, minsize)

	ralg, err := ro.resampling.rioAlg()
	if err != nil {
		return err
	}
	cgc := createCGOContext(ro.config, ro.errorHandler)
	C.godalDatasetRasterIO(cgc.cPointer(), ds.handle(), C.GDALRWFlag(rw),
		C.int(srcX), C.int(srcY), C.int(ro.dsWidth), C.int(ro.dsHeight),
		cBuf,
		C.int(bufWidth), C.int(bufHeight), C.GDALDataType(dtype),
		C.int(len(ro.bands)), cIntArray(ro.bands),
		C.int(pixelSpacing), C.int(lineSpacing), C.int(bandSpacing), ralg)
	return cgc.close()
}

// RegisterAll calls GDALAllRegister which registers all available raster and vector
// drivers.
//
// Alternatively, you may also register a select number of drivers by calling one or more of
//   - godal.RegisterInternal() // MEM, VRT, GTiff and GeoJSON
//   - godal.RegisterRaster(godal.GTiff,godal.VRT)
//   - godal.RegisterVector(godal.Shapefile)
func RegisterAll() {
	C.GDALAllRegister()
}

func RegisterPlugins() {
	C.godalRegisterPlugins()
}

func RegisterPlugin(name string, opts ...RegisterPluginOption) error {
	ro := registerPluginOpts{}
	for _, o := range opts {
		o.setRegisterPluginOpt(&ro)
	}
	cgc := createCGOContext(nil, ro.errorHandler)
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	C.godalRegisterPlugin(cgc.cPointer(), cname)
	return cgc.close()
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

// LongName returns the driver long name.
func (drv Driver) LongName() string {
	return C.GoString(C.GDALGetDriverLongName(drv.handle()))
}

// ShortName returns the driver short name.
func (drv Driver) ShortName() string {
	return C.GoString(C.GDALGetDriverShortName(drv.handle()))
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
	cname := C.CString(name)
	defer createOpts.free()
	defer C.free(unsafe.Pointer(cname))

	cgc := createCGOContext(gopts.config, gopts.errorHandler)
	hndl := C.godalCreate(cgc.cPointer(), drv.handle(), (*C.char)(unsafe.Pointer(cname)),
		C.int(width), C.int(height), C.int(nBands), C.GDALDataType(dtype),
		createOpts.cPointer())

	if err := cgc.close(); err != nil {
		return nil, err
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
	cname := C.CString(name)
	defer createOpts.free()
	defer C.free(unsafe.Pointer(cname))

	cgc := createCGOContext(gopts.config, gopts.errorHandler)
	hndl := C.godalCreate(cgc.cPointer(), drv.handle(), (*C.char)(unsafe.Pointer(cname)),
		0, 0, 0, C.GDT_Unknown, createOpts.cPointer())
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil

}

type majorObject struct {
	cHandle C.GDALMajorObjectH
}

// Dataset is a wrapper around a GDALDatasetH
type Dataset struct {
	majorObject
}

// handle returns a pointer to the underlying GDALDatasetH
func (ds *Dataset) handle() C.GDALDatasetH {
	return C.GDALDatasetH(ds.majorObject.cHandle)
}

// Open calls GDALOpenEx() with the provided options. It returns nil and an error
// in case there was an error opening the provided dataset name.
//
// name may be a filename or any supported string supported by gdal (e.g. a /vsixxx path,
// the xml string representing a vrt dataset, etc...)
func Open(name string, options ...OpenOption) (*Dataset, error) {
	oopts := openOpts{
		flags:        C.GDAL_OF_READONLY | C.GDAL_OF_VERBOSE_ERROR,
		siblingFiles: []string{filepath.Base(name)},
	}
	for _, opt := range options {
		opt.setOpenOpt(&oopts)
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

// Close releases the dataset
func (ds *Dataset) Close(opts ...CloseOption) error {
	co := &closeOpts{}
	for _, o := range opts {
		o.setCloseOpt(co)
	}
	if ds.cHandle == nil {
		return fmt.Errorf("close called more than once")
	}
	cgc := createCGOContext(nil, co.errorHandler)
	C.godalClose(cgc.cPointer(), ds.handle())
	ds.cHandle = nil
	return cgc.close()
}

// LibVersion is the GDAL lib versioning scheme
type LibVersion int

// Major returns the GDAL major version (e.g. "3" in 3.2.1)
func (lv LibVersion) Major() int {
	return int(lv) / 1000000
}

// Minor return the GDAL minor version (e.g. "2" in 3.2.1)
func (lv LibVersion) Minor() int {
	return (int(lv) - lv.Major()*1000000) / 10000
}

// Revision returns the GDAL revision number (e.g. "1" in 3.2.1)
func (lv LibVersion) Revision() int {
	return (int(lv) - lv.Major()*1000000 - lv.Minor()*10000) / 100
}

// AssertMinVersion will panic if the runtime version is not at least major.minor.revision
func AssertMinVersion(major, minor, revision int) {
	if !CheckMinVersion(major, minor, revision) {
		runtimeVersion := Version()
		panic(fmt.Errorf("runtime version %d.%d.%d < %d.%d.%d",
			runtimeVersion.Major(), runtimeVersion.Minor(), runtimeVersion.Revision(), major, minor, revision))
	}
}

// CheckMinVersion will return true if the runtime version is at least major.minor.revision
func CheckMinVersion(major, minor, revision int) bool {
	runtimeVersion := Version()
	if runtimeVersion.Major() < major ||
		(runtimeVersion.Major() == major && runtimeVersion.Minor() < minor) ||
		(runtimeVersion.Major() == major && runtimeVersion.Minor() == minor && runtimeVersion.Revision() < revision) {
		return false
	}
	return true
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
		lfn.err = combine(lfn.err, err)
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

// ResamplingAlg is a resampling method
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

func gridAlgFromString(str string) (C.GDALGridAlgorithm, error) {
	switch str {
	case "invdist":
		return C.GGA_InverseDistanceToAPower, nil
	case "average":
		return C.GGA_MovingAverage, nil
	case "nearest":
		return C.GGA_NearestNeighbor, nil
	case "minimum":
		return C.GGA_MetricMinimum, nil
	case "maximum":
		return C.GGA_MetricMaximum, nil
	case "range":
		return C.GGA_MetricRange, nil
	case "count":
		return C.GGA_MetricCount, nil
	case "average_distance":
		return C.GGA_MetricAverageDistance, nil
	case "average_distance_pts":
		return C.GGA_MetricAverageDistancePts, nil
	case "linear":
		return C.GGA_Linear, nil
	case "invdistnn":
		return C.GGA_InverseDistanceToAPowerNearestNeighbor, nil
	default:
		return C.GGA_InverseDistanceToAPower, fmt.Errorf("unknown gridding algorithm %s", str)
	}
}

func bufferType(buffer interface{}) DataType {
	switch buffer.(type) {
	case []byte:
		return Byte
	case []int8:
		return Int8
	case []int16:
		return Int16
	case []uint16:
		return UInt16
	case []int32:
		return Int32
	case []uint32:
		return UInt32
	case []float32:
		return Float32
	case []float64:
		return Float64
	case []complex64:
		return CFloat32
	case []complex128:
		return CFloat64
	default:
		panic("unsupported type")
	}
}

// cBuffer returns the type of an individual element, and a pointer to the
// underlying memory array
func cBuffer(buffer interface{}, minsize int) unsafe.Pointer {
	sizecheck := func(size int) {
		if size < minsize {
			panic(fmt.Sprintf("buffer len=%d less than min=%d", size, minsize))
		}
	}
	switch buf := buffer.(type) {
	case []byte:
		sizecheck(len(buf))
		return unsafe.Pointer(&buf[0])
	case []int8:
		sizecheck(len(buf))
		return unsafe.Pointer(&buf[0])
	case []int16:
		sizecheck(len(buf))
		return unsafe.Pointer(&buf[0])
	case []uint16:
		sizecheck(len(buf))
		return unsafe.Pointer(&buf[0])
	case []int32:
		sizecheck(len(buf))
		return unsafe.Pointer(&buf[0])
	case []uint32:
		sizecheck(len(buf))
		return unsafe.Pointer(&buf[0])
	case []float32:
		sizecheck(len(buf))
		return unsafe.Pointer(&buf[0])
	case []float64:
		sizecheck(len(buf))
		return unsafe.Pointer(&buf[0])
	case []complex64:
		sizecheck(len(buf))
		return unsafe.Pointer(&buf[0])
	case []complex128:
		sizecheck(len(buf))
		return unsafe.Pointer(&buf[0])
	default:
		panic("unsupported type")
	}
}

func (mo majorObject) Metadata(key string, opts ...MetadataOption) string {
	mopts := metadataOpts{}
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
	mopts := metadataOpts{}
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
	mopts := metadataOpts{}
	for _, opt := range opts {
		opt.setMetadataOpt(&mopts)
	}
	ckey := C.CString(key)
	cval := C.CString(value)
	cdom := C.CString(mopts.domain)
	defer C.free(unsafe.Pointer(ckey))
	defer C.free(unsafe.Pointer(cdom))
	defer C.free(unsafe.Pointer(cval))
	cgc := createCGOContext(nil, mopts.errorHandler)
	C.godalSetMetadataItem(cgc.cPointer(), mo.cHandle, ckey, cval, cdom)
	return cgc.close()
}

func (mo majorObject) ClearMetadata(opts ...MetadataOption) error {
	mopts := metadataOpts{}
	for _, opt := range opts {
		opt.setMetadataOpt(&mopts)
	}
	cdom := C.CString(mopts.domain)
	defer C.free(unsafe.Pointer(cdom))
	cgc := createCGOContext(nil, mopts.errorHandler)
	C.godalClearMetadata(cgc.cPointer(), mo.cHandle, cdom)
	return cgc.close()
}

func (mo majorObject) MetadataDomains() []string {
	strs := C.GDALGetMetadataDomainList(mo.cHandle)
	return cStringArrayToSlice(strs)
}

// Description returns the description/name
func (mo majorObject) Description() string {
	desc := C.GDALGetDescription(mo.cHandle)
	return C.GoString(desc)
}

// SetDescription sets the description
func (mo majorObject) SetDescription(description string, opts ...SetDescriptionOption) error {
	scio := &setDescriptionOpts{}
	for _, opt := range opts {
		opt.setDescriptionOpt(scio)
	}

	cgc := createCGOContext(nil, scio.errorHandler)
	cname := unsafe.Pointer(C.CString(description))
	defer C.free(cname)
	C.godalSetDescription(cgc.cPointer(), mo.cHandle, (*C.char)(cname))
	return cgc.close()
}

type openUpdateOpt struct{}

// Update is an OpenOption that instructs gdal to open the dataset for writing/updating
func Update() interface {
	OpenOption
} {
	return openUpdateOpt{}
}

func (openUpdateOpt) setOpenOpt(oo *openOpts) {
	//unset readonly
	oo.flags = oo.flags &^ C.GDAL_OF_READONLY //actually a noop as OF_READONLY is 0
	oo.flags |= C.GDAL_OF_UPDATE
}

type openSharedOpt struct{}

// Shared opens the dataset with OF_OPEN_SHARED
func Shared() interface {
	OpenOption
} {
	return openSharedOpt{}
}

func (openSharedOpt) setOpenOpt(oo *openOpts) {
	oo.flags |= C.GDAL_OF_SHARED
}

type vectorOnlyOpt struct{}

// VectorOnly limits drivers to vector ones (incompatible with RasterOnly() )
func VectorOnly() interface {
	OpenOption
} {
	return vectorOnlyOpt{}
}
func (vectorOnlyOpt) setOpenOpt(oo *openOpts) {
	oo.flags |= C.GDAL_OF_VECTOR
}

type rasterOnlyOpt struct{}

// RasterOnly limits drivers to vector ones (incompatible with VectorOnly() )
func RasterOnly() interface {
	OpenOption
} {
	return rasterOnlyOpt{}
}
func (rasterOnlyOpt) setOpenOpt(oo *openOpts) {
	oo.flags |= C.GDAL_OF_RASTER
}

// SpatialRef is a wrapper around OGRSpatialReferenceH
type SpatialRef struct {
	handle  C.OGRSpatialReferenceH
	isOwned bool
}

// WKT returns spatialrefernece as WKT
func (sr *SpatialRef) WKT(opts ...WKTExportOption) (string, error) {
	wo := &srWKTOpts{}
	for _, o := range opts {
		o.setWKTExportOpt(wo)
	}
	cgc := createCGOContext(nil, wo.errorHandler)
	cwkt := C.godalExportToWKT(cgc.cPointer(), sr.handle)
	if err := cgc.close(); err != nil {
		return "", err
	}
	wkt := C.GoString(cwkt)
	C.CPLFree(unsafe.Pointer(cwkt))
	return wkt, nil
}

// Close releases memory
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

// NewSpatialRef creates a SpatialRef from any "user" projection string, e.g.
// "epsg:4326", "+proj=lonlat", wkt, wkt2 or projjson (as supported by
// gdal's OSRCreateFromUserInput
func NewSpatialRef(userInput string, opts ...CreateSpatialRefOption) (*SpatialRef, error) {
	cso := &createSpatialRefOpts{}
	for _, o := range opts {
		o.setCreateSpatialRefOpt(cso)
	}
	cstr := C.CString(userInput)
	defer C.free(unsafe.Pointer(cstr))
	cgc := createCGOContext(nil, cso.errorHandler)
	hndl := C.godalCreateUserSpatialRef(cgc.cPointer(), (*C.char)(unsafe.Pointer(cstr)))
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &SpatialRef{handle: hndl, isOwned: true}, nil
}

// NewSpatialRefFromWKT creates a SpatialRef from an opengis WKT description
func NewSpatialRefFromWKT(wkt string, opts ...CreateSpatialRefOption) (*SpatialRef, error) {
	cso := &createSpatialRefOpts{}
	for _, o := range opts {
		o.setCreateSpatialRefOpt(cso)
	}
	cstr := C.CString(wkt)
	defer C.free(unsafe.Pointer(cstr))
	cgc := createCGOContext(nil, cso.errorHandler)
	hndl := C.godalCreateWKTSpatialRef(cgc.cPointer(), (*C.char)(unsafe.Pointer(cstr)))
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &SpatialRef{handle: hndl, isOwned: true}, nil
}

// NewSpatialRefFromProj4 creates a SpatialRef from a proj4 string
func NewSpatialRefFromProj4(proj string, opts ...CreateSpatialRefOption) (*SpatialRef, error) {
	cso := &createSpatialRefOpts{}
	for _, o := range opts {
		o.setCreateSpatialRefOpt(cso)
	}
	cstr := C.CString(proj)
	defer C.free(unsafe.Pointer(cstr))
	cgc := createCGOContext(nil, cso.errorHandler)
	hndl := C.godalCreateProj4SpatialRef(cgc.cPointer(), (*C.char)(unsafe.Pointer(cstr)))
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &SpatialRef{handle: hndl, isOwned: true}, nil
}

// NewSpatialRefFromEPSG creates a SpatialRef from an epsg code
func NewSpatialRefFromEPSG(code int, opts ...CreateSpatialRefOption) (*SpatialRef, error) {
	cso := &createSpatialRefOpts{}
	for _, o := range opts {
		o.setCreateSpatialRefOpt(cso)
	}
	cgc := createCGOContext(nil, cso.errorHandler)
	hndl := C.godalCreateEPSGSpatialRef(cgc.cPointer(), C.int(code))
	if err := cgc.close(); err != nil {
		return nil, err
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
	to := &trnOpts{}
	for _, o := range opts {
		o.setTransformOpt(to)
	}
	cgc := createCGOContext(nil, to.errorHandler)
	hndl := C.godalNewCoordinateTransformation(cgc.cPointer(), src.handle, dst.handle)
	if err := cgc.close(); err != nil {
		return nil, err
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

// EPSGTreatsAsLatLong returns TRUE if EPSG feels the SpatialRef should be treated as having lat/long coordinate ordering.
func (sr *SpatialRef) EPSGTreatsAsLatLong() bool {
	ret := C.OSREPSGTreatsAsLatLong(sr.handle)
	return ret != 0
}

// Geographic returns wether the SpatialRef is geographic
func (sr *SpatialRef) Geographic() bool {
	ret := C.OSRIsGeographic(sr.handle)
	return ret != 0
}

// Projected returns wether the SpatialRef is projected
func (sr *SpatialRef) Projected() bool {
	ret := C.OSRIsProjected(sr.handle)
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

// AttrValue Fetch indicated attribute of named node from within the WKT tree.
func (sr *SpatialRef) AttrValue(name string, child int) (string, bool) {
	cstr := C.CString(name)
	defer C.free(unsafe.Pointer(cstr))
	cret := C.OSRGetAttrValue(sr.handle, cstr, C.int(child))
	if cret != nil {
		return C.GoString(cret), true
	}
	return "", false
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

// Validate SRS tokens.
func (sr *SpatialRef) Validate(opts ...SpatialRefValidateOption) error {
	vo := spatialRefValidateOpts{}
	for _, opt := range opts {
		opt.setSpatialRefValidateOpt(&vo)
	}
	cgc := createCGOContext(nil, vo.errorHandler)
	C.godalValidateSpatialRef(cgc.cPointer(), sr.handle)
	return cgc.close()
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
	cname := unsafe.Pointer(C.CString(dstDS))
	defer C.free(cname)

	cgc := createCGOContext(gopts.config, gopts.errorHandler)
	hndl := C.godalRasterize(cgc.cPointer(), (*C.char)(cname), nil, ds.handle(), cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// RasterizeInto wraps GDALRasterize() and rasterizes the provided vectorDataset into the ds Dataset
func (ds *Dataset) RasterizeInto(vectorDS *Dataset, switches []string, opts ...RasterizeIntoOption) error {
	gopts := rasterizeIntoOpts{}
	for _, opt := range opts {
		opt.setRasterizeIntoOpt(&gopts)
	}
	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()

	cgc := createCGOContext(gopts.config, gopts.errorHandler)
	C.godalRasterize(cgc.cPointer(), nil, ds.handle(), vectorDS.handle(), cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return err
	}
	return nil
}

// RasterizeGeometry "burns" the provided geometry onto ds.
// By default, the "0" value is burned into all of ds's bands. This behavior can be modified
// with the following options:
//   - Bands(bnd ...int) the list of bands to affect
//   - Values(val ...float64) the pixel value to burn. There must be either 1 or len(bands) values
//
// provided
//   - AllTouched() pixels touched by lines or polygons will be updated, not just those on the line
//
// render path, or whose center point is within the polygon.
func (ds *Dataset) RasterizeGeometry(g *Geometry, opts ...RasterizeGeometryOption) error {
	opt := rasterizeGeometryOpts{}
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
	cgc := createCGOContext(nil, opt.errorHandler)
	C.godalRasterizeGeometry(cgc.cPointer(), ds.handle(), g.handle,
		cIntArray(opt.bands), C.int(len(opt.bands)), cDoubleArray(opt.values), C.int(opt.allTouched))
	return cgc.close()
}

// GeometryType is a geometry type
type GeometryType uint32

const (
	//GTUnknown is a GeometryType
	GTUnknown = GeometryType(C.wkbUnknown)
	//GTPoint is a GeometryType
	GTPoint = GeometryType(C.wkbPoint)
	//GTPoint25D is a GeometryType
	GTPoint25D = GeometryType(C.wkbPoint25D)
	//GTLinearRing is a GeometryType
	GTLinearRing = GeometryType(C.wkbLinearRing)
	//GTLineString is a GeometryType
	GTLineString = GeometryType(C.wkbLineString)
	//GTLineString25D is a GeometryType
	GTLineString25D = GeometryType(C.wkbLineString25D)
	//GTPolygon is a GeometryType
	GTPolygon = GeometryType(C.wkbPolygon)
	//GTPolygon25D is a GeometryType
	GTPolygon25D = GeometryType(C.wkbPolygon25D)
	//GTMultiPoint is a GeometryType
	GTMultiPoint = GeometryType(C.wkbMultiPoint)
	//GTMultiPoint25D is a GeometryType
	GTMultiPoint25D = GeometryType(C.wkbMultiPoint25D)
	//GTMultiLineString is a GeometryType
	GTMultiLineString = GeometryType(C.wkbMultiLineString)
	//GTMultiLineString25D is a GeometryType
	GTMultiLineString25D = GeometryType(C.wkbMultiLineString25D)
	//GTMultiPolygon is a GeometryType
	GTMultiPolygon = GeometryType(C.wkbMultiPolygon)
	//GTMultiPolygon25D is a GeometryType
	GTMultiPolygon25D = GeometryType(C.wkbMultiPolygon25D)
	//GTGeometryCollection is a GeometryType
	GTGeometryCollection = GeometryType(C.wkbGeometryCollection)
	//GTGeometryCollection25D is a GeometryType
	GTGeometryCollection25D = GeometryType(C.wkbGeometryCollection25D)
	//GTNone is a GeometryType
	GTNone = GeometryType(C.wkbNone)
)

// FieldType is a vector field (attribute/column) type
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
	//FTUnknown allow to handle deprecated types like WideString or WideStringList
	FTUnknown = FieldType(C.OFTMaxType + 1)
)

// FieldDefinition defines a single attribute
type FieldDefinition struct {
	name  string
	ftype FieldType
}

// NewFieldDefinition creates a FieldDefinition
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
//
//	 []string{
//	   "-f", "GeoJSON",
//		  "-t_srs","epsg:3857",
//	   "-dstalpha"}
//
// Creation options and Driver may be set either in the switches slice with
//
//	switches:=[]string{"-dsco","TILED=YES", "-f","GeoJSON"}
//
// or through Options with
//
//	ds.VectorTranslate(dst, switches, CreationOption("TILED=YES","BLOCKXSIZE=256"), GeoJSON)
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
	cname := unsafe.Pointer(C.CString(dstDS))
	defer C.free(cname)

	cgc := createCGOContext(gopts.config, gopts.errorHandler)
	hndl := C.godalDatasetVectorTranslate(cgc.cPointer(), (*C.char)(cname), ds.handle(), cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return nil, err
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

// Name returns the layer name
func (layer Layer) Name() string {
	return C.GoString(C.OGR_L_GetName(layer.handle()))
}

// Type returns the layer geometry type.
func (layer Layer) Type() GeometryType {
	return GeometryType(C.OGR_L_GetGeomType(layer.handle()))
}

// Bounds returns the layer's envelope in the order minx,miny,maxx,maxy
func (layer Layer) Bounds(opts ...BoundsOption) ([4]float64, error) {
	bo := boundsOpts{}
	for _, o := range opts {
		o.setBoundsOpt(&bo)
	}
	var env C.OGREnvelope
	cgc := createCGOContext(nil, bo.errorHandler)
	C.godalLayerGetExtent(cgc.cPointer(), layer.handle(), &env)
	if err := cgc.close(); err != nil {
		return [4]float64{}, err
	}
	bnds := [4]float64{
		float64(env.MinX),
		float64(env.MinY),
		float64(env.MaxX),
		float64(env.MaxY),
	}
	if bo.sr == nil {
		return bnds, nil
	}
	sr := layer.SpatialRef()
	defer sr.Close()
	bnds, err := reprojectBounds(bnds, sr, bo.sr)
	if err != nil {
		return [4]float64{}, err
	}
	return bnds, nil
}

// FeatureCount returns the number of features in the layer
func (layer Layer) FeatureCount(opts ...FeatureCountOption) (int, error) {
	fco := &featureCountOpts{}
	for _, o := range opts {
		o.setFeatureCountOpt(fco)
	}
	var count C.int
	cgc := createCGOContext(nil, fco.errorHandler)
	C.godalLayerFeatureCount(cgc.cPointer(), layer.handle(), &count)
	if err := cgc.close(); err != nil {
		return 0, err
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

// Area computes the area for geometries of type LinearRing, Polygon or MultiPolygon (returns zero for other types).
// The area is in square units of the spatial reference system in use.
func (g *Geometry) Area() float64 {
	return float64(C.OGR_G_Area(g.handle))
}

// Name fetch WKT name for geometry type.
func (g *Geometry) Name() string {
	return C.GoString(C.OGR_G_GetGeometryName(g.handle))
}

// GeometryCount fetch the number of elements in a geometry or number of geometries in container.
// Only geometries of type Polygon, MultiPoint, MultiLineString, MultiPolygon or GeometryCollection may return a valid value.
// Other geometry types will silently return 0.
// For a polygon, the returned number is the number of rings (exterior ring + interior rings).
func (g *Geometry) GeometryCount() int {
	return int(C.OGR_G_GetGeometryCount(g.handle))
}

// Type fetch geometry type.
func (g *Geometry) Type() GeometryType {
	return GeometryType(C.OGR_G_GetGeometryType(g.handle))
}

// Simplify simplifies the geometry with the given tolerance
func (g *Geometry) Simplify(tolerance float64, opts ...SimplifyOption) (*Geometry, error) {
	so := &simplifyOpts{}
	for _, o := range opts {
		o.setSimplifyOpt(so)
	}
	cgc := createCGOContext(nil, so.errorHandler)
	hndl := C.godal_OGR_G_Simplify(cgc.cPointer(), g.handle, C.double(tolerance))
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}, nil
}

// Buffer buffers the geometry
func (g *Geometry) Buffer(distance float64, segments int, opts ...BufferOption) (*Geometry, error) {
	bo := &bufferOpts{}
	for _, o := range opts {
		o.setBufferOpt(bo)
	}
	cgc := createCGOContext(nil, bo.errorHandler)
	hndl := C.godal_OGR_G_Buffer(cgc.cPointer(), g.handle, C.double(distance), C.int(segments))
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}, nil
}

// Difference generates a new geometry which is the region of this geometry with the region of the other geometry removed.
func (g *Geometry) Difference(other *Geometry, opts ...DifferenceOption) (*Geometry, error) {
	// If other geometry is nil, GDAL crashes
	if other == nil || other.handle == nil {
		return nil, errors.New("other geometry is empty")
	}
	do := &differenceOpts{}
	for _, o := range opts {
		o.setDifferenceOpt(do)
	}
	cgc := createCGOContext(nil, do.errorHandler)
	hndl := C.godal_OGR_G_Difference(cgc.cPointer(), g.handle, other.handle)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}, nil
}

// AddGeometry add a geometry to a geometry container.
func (g *Geometry) AddGeometry(subGeom *Geometry, opts ...AddGeometryOption) error {
	ago := &addGeometryOpts{}
	for _, o := range opts {
		o.setAddGeometryOpt(ago)
	}
	cgc := createCGOContext(nil, ago.errorHandler)
	C.godal_OGR_G_AddGeometry(cgc.cPointer(), g.handle, subGeom.handle)
	return cgc.close()
}

// ForceToMultiPolygon convert to multipolygon.
func (g *Geometry) ForceToMultiPolygon() *Geometry {
	hndl := C.OGR_G_ForceToMultiPolygon(g.handle)
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}
}

// ForceToPolygon convert to polygon.
func (g *Geometry) ForceToPolygon() *Geometry {
	hndl := C.OGR_G_ForceToPolygon(g.handle)
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}
}

// SubGeometry Fetch geometry from a geometry container.
func (g *Geometry) SubGeometry(subGeomIndex int, opts ...SubGeometryOption) (*Geometry, error) {
	so := &subGeometryOpts{}
	for _, o := range opts {
		o.setSubGeometryOpt(so)
	}
	cgc := createCGOContext(nil, so.errorHandler)
	hndl := C.godal_OGR_G_GetGeometryRef(cgc.cPointer(), g.handle, C.int(subGeomIndex))
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{
		isOwned: false,
		handle:  hndl,
	}, nil
}

// Intersects determines whether two geometries intersect. If GEOS is enabled, then
// this is done in rigorous fashion otherwise TRUE is returned if the
// envelopes (bounding boxes) of the two geometries overlap.
func (g *Geometry) Intersects(other *Geometry, opts ...IntersectsOption) (bool, error) {
	bo := &intersectsOpts{}
	for _, o := range opts {
		o.setIntersectsOpt(bo)
	}
	cgc := createCGOContext(nil, bo.errorHandler)
	ret := C.godal_OGR_G_Intersects(cgc.cPointer(), g.handle, other.handle)
	if err := cgc.close(); err != nil {
		return false, err
	}
	return ret != 0, nil
}

// Intersection generates a new geometry which is the region of intersection of the two geometries operated on.
func (g *Geometry) Intersection(other *Geometry, opts ...IntersectionOption) (*Geometry, error) {
	// If other geometry is nil, GDAL crashes
	if other == nil || other.handle == nil {
		return nil, errors.New("other geometry is empty")
	}
	io := &intersectionOpts{}
	for _, o := range opts {
		o.setIntersectionOpt(io)
	}
	cgc := createCGOContext(nil, io.errorHandler)
	hndl := C.godal_OGR_G_Intersection(cgc.cPointer(), g.handle, other.handle)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}, nil
}

// Union generates a new geometry which is the region of union of the two geometries operated on.
func (g *Geometry) Union(other *Geometry, opts ...UnionOption) (*Geometry, error) {
	// If other geometry is nil, GDAL crashes
	if other == nil || other.handle == nil {
		return nil, errors.New("other geometry is empty")
	}
	uo := &unionOpts{}
	for _, o := range opts {
		o.setUnionOpt(uo)
	}
	cgc := createCGOContext(nil, uo.errorHandler)
	hndl := C.godal_OGR_G_Union(cgc.cPointer(), g.handle, other.handle)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}, nil
}

// Contains tests if this geometry contains the other geometry.
func (g *Geometry) Contains(other *Geometry) bool {
	ret := C.OGR_G_Contains(g.handle, other.handle)
	return ret != 0
}

// Empty returns true if the geometry is empty
func (g *Geometry) Empty() bool {
	ret := C.OGR_G_IsEmpty(g.handle)
	return ret != 0
}

// Valid returns true is the geometry is valid
func (g *Geometry) Valid() bool {
	ret := C.OGR_G_IsValid(g.handle)
	return ret != 0
}

// Bounds returns the geometry's envelope in the order minx,miny,maxx,maxy
func (g *Geometry) Bounds(opts ...BoundsOption) ([4]float64, error) {
	bo := boundsOpts{}
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

// Feature is a Layer feature
type Feature struct {
	handle C.OGRFeatureH
}

// Geometry returns a handle to the feature's geometry
func (f *Feature) Geometry() *Geometry {
	hndl := C.OGR_F_GetGeometryRef(f.handle)
	return &Geometry{
		isOwned: false,
		handle:  hndl,
	}
}

// SetGeometry overwrites the feature's geometry
func (f *Feature) SetGeometry(geom *Geometry, opts ...SetGeometryOption) error {
	sgo := &setGeometryOpts{}
	for _, o := range opts {
		o.setSetGeometryOpt(sgo)
	}
	cgc := createCGOContext(nil, sgo.errorHandler)
	C.godalFeatureSetGeometry(cgc.cPointer(), f.handle, geom.handle)
	return cgc.close()
}

// SetGeometryColumnName set the name of feature first geometry field.
// Deprecated when running with GDAL 3.6+, use SetGeometryColumnName on Layer instead.
// No more supported when running with GDAL 3.9+.
func (f *Feature) SetGeometryColumnName(name string, opts ...SetGeometryColumnNameOption) error {
	so := &setGeometryColumnNameOpts{}
	for _, o := range opts {
		o.setGeometryColumnNameOpt(so)
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	cgc := createCGOContext(nil, so.errorHandler)
	C.godalFeatureSetGeometryColumnName(cgc.cPointer(), f.handle, (*C.char)(cname))
	return cgc.close()
}

// SetFID set feature identifier
func (f *Feature) SetFID(fid int64) {
	// OGR error returned is always none, so we don't handle it
	C.OGR_F_SetFID(f.handle, C.GIntBig(fid))
}

// SetFieldValue set feature's field value
func (f *Feature) SetFieldValue(field Field, value interface{}, opts ...SetFieldValueOption) error {
	sfvo := &setFieldValueOpts{}
	for _, o := range opts {
		o.setSetFieldValueOpt(sfvo)
	}
	cgc := createCGOContext(nil, sfvo.errorHandler)

	switch field.ftype {
	case FTInt:
		intValue, ok := value.(int)
		if !ok {
			return errors.New("value for this field must be of type 'int'")
		}
		C.godalFeatureSetFieldInteger(cgc.cPointer(), f.handle, C.int(field.index), C.int(intValue))
	case FTInt64:
		int64Value, ok := value.(int64)
		if !ok {
			return errors.New("value for this field must be of type 'int64'")
		}
		C.godalFeatureSetFieldInteger64(cgc.cPointer(), f.handle, C.int(field.index), C.longlong(int64Value))
	case FTReal:
		floatValue, ok := value.(float64)
		if !ok {
			return errors.New("value for this field must be of type 'float64'")
		}
		C.godalFeatureSetFieldDouble(cgc.cPointer(), f.handle, C.int(field.index), C.double(floatValue))
	case FTString:
		stringValue, ok := value.(string)
		if !ok {
			return errors.New("value for this field must be of type 'string'")
		}
		cval := C.CString(stringValue)
		defer C.free(unsafe.Pointer(cval))
		C.godalFeatureSetFieldString(cgc.cPointer(), f.handle, C.int(field.index), cval)
	case FTDate, FTTime, FTDateTime:
		timeValue, ok := value.(time.Time)
		if !ok {
			return errors.New("value for this field must be of type 'time.Time'")
		}
		timeZone := 0 // 0=unknown, 1=localtime, 100=GMT, 101=GMT+15minute, 99=GMT-15minute...
		if timeValue.Location() == time.Local {
			timeZone = 1
		} else {
			_, offset := timeValue.Zone()
			timeZone = offset/60/15 + 100
		}
		C.godalFeatureSetFieldDateTime(
			cgc.cPointer(),
			f.handle,
			C.int(field.index),
			C.int(timeValue.Year()),
			C.int(timeValue.Month()),
			C.int(timeValue.Day()),
			C.int(timeValue.Hour()),
			C.int(timeValue.Minute()),
			C.int(timeValue.Second()),
			C.int(timeZone),
		)
	case FTIntList:
		intListValue, ok := value.([]int)
		if !ok {
			return errors.New("value for this field must be of type '[]int'")
		}
		C.godalFeatureSetFieldIntegerList(cgc.cPointer(), f.handle, C.int(field.index), C.int(len(intListValue)), cIntArray(intListValue))
	case FTInt64List:
		int64ListValue, ok := value.([]int64)
		if !ok {
			return errors.New("value for this field must be of type '[]int64'")
		}
		C.godalFeatureSetFieldInteger64List(cgc.cPointer(), f.handle, C.int(field.index), C.int(len(int64ListValue)), cLongArray(int64ListValue))
	case FTRealList:
		float64ListValue, ok := value.([]float64)
		if !ok {
			return errors.New("value for this field must be of type '[]float64'")
		}
		C.godalFeatureSetFieldDoubleList(cgc.cPointer(), f.handle, C.int(field.index), C.int(len(float64ListValue)), cDoubleArray(float64ListValue))
	case FTStringList:
		stringListValue, ok := value.([]string)
		if !ok {
			return errors.New("value for this field must be of type '[]float64'")
		}
		cArray := sliceToCStringArray(stringListValue)
		C.godalFeatureSetFieldStringList(cgc.cPointer(), f.handle, C.int(field.index), cArray.cPointer())
		cArray.free()
	case FTBinary:
		bytesValue, ok := value.([]byte)
		if !ok {
			return errors.New("value for this field must be of type '[]byte'")
		}
		C.godalFeatureSetFieldBinary(cgc.cPointer(), f.handle, C.int(field.index), C.int(len(bytesValue)), unsafe.Pointer(&bytesValue[0]))
	default:
		cgc.close() //avoid resource leak
		return errors.New("setting value is not implemented for this type of field")
	}

	return cgc.close()
}

// Field is a Feature attribute
type Field struct {
	index int
	isSet bool
	ftype FieldType
	val   interface{}
}

// IsSet returns if the field has ever been assigned a value or not.
func (fld Field) IsSet() bool {
	return fld.isSet
}

// Type returns the field's native type
func (fld Field) Type() FieldType {
	return fld.ftype
}

// Int returns the Field as an integer
func (fld Field) Int() int64 {
	switch fld.ftype {
	case FTInt, FTInt64:
		return fld.val.(int64)
	case FTReal:
		return int64(fld.val.(float64))
	case FTString:
		ii, _ := strconv.Atoi(fld.val.(string))
		return int64(ii)
	default:
		return 0
	}
}

// Float returns the field as a float64
func (fld Field) Float() float64 {
	switch fld.ftype {
	case FTInt, FTInt64:
		return float64(fld.val.(int64))
	case FTReal:
		return fld.val.(float64)
	case FTString:
		ii, _ := strconv.ParseFloat(fld.val.(string), 64)
		return ii
	default:
		return 0
	}
}

// String returns the field as a string
func (fld Field) String() string {
	switch fld.ftype {
	case FTInt, FTInt64:
		return fmt.Sprintf("%d", fld.val.(int64))
	case FTReal:
		return fmt.Sprintf("%f", fld.val.(float64))
	case FTString:
		return fld.val.(string)
	default:
		return ""
	}
}

// Bytes returns the field as a byte slice
func (fld Field) Bytes() []byte {
	switch fld.ftype {
	case FTBinary:
		return fld.val.([]byte)
	default:
		return nil
	}
}

// DateTime returns the field as a date time
func (fld Field) DateTime() *time.Time {
	switch fld.ftype {
	case FTDate, FTTime, FTDateTime:
		return fld.val.(*time.Time)
	default:
		return nil
	}
}

// IntList returns the field as a list of integer
func (fld Field) IntList() []int64 {
	switch fld.ftype {
	case FTIntList, FTInt64List:
		return fld.val.([]int64)
	default:
		return nil
	}
}

// FloatList returns the field as a list of float64
func (fld Field) FloatList() []float64 {
	switch fld.ftype {
	case FTRealList:
		return fld.val.([]float64)
	default:
		return nil
	}
}

// StringList returns the field as a list of string
func (fld Field) StringList() []string {
	switch fld.ftype {
	case FTStringList:
		return fld.val.([]string)
	default:
		return nil
	}
}

// Fields returns all the Feature's fields
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
		fld := Field{
			index: int(fid),
			isSet: C.OGR_F_IsFieldSet(f.handle, fid) != 0,
		}
		switch ftype {
		case C.OFTInteger:
			fld.ftype = FTInt
			fld.val = int64(C.OGR_F_GetFieldAsInteger64(f.handle, fid))
		case C.OFTInteger64:
			fld.ftype = FTInt64
			fld.val = int64(C.OGR_F_GetFieldAsInteger64(f.handle, fid))
		case C.OFTReal:
			fld.ftype = FTReal
			fld.val = float64(C.OGR_F_GetFieldAsDouble(f.handle, fid))
		case C.OFTString:
			fld.ftype = FTString
			fld.val = C.GoString(C.OGR_F_GetFieldAsString(f.handle, fid))
		case C.OFTDate:
			fld.ftype = FTDate
			fld.val = f.getFieldAsDateTime(fid)
		case C.OFTTime:
			fld.ftype = FTTime
			fld.val = f.getFieldAsDateTime(fid)
		case C.OFTDateTime:
			fld.ftype = FTDateTime
			fld.val = f.getFieldAsDateTime(fid)
		case C.OFTIntegerList:
			fld.ftype = FTIntList
			var length C.int
			cArray := C.OGR_F_GetFieldAsIntegerList(f.handle, fid, &length)
			fld.val = cIntArrayToSlice(cArray, length)
		case C.OFTInteger64List:
			fld.ftype = FTInt64List
			var length C.int
			cArray := C.OGR_F_GetFieldAsInteger64List(f.handle, fid, &length)
			fld.val = cLongArrayToSlice(cArray, length)
		case C.OFTRealList:
			fld.ftype = FTRealList
			var length C.int
			cArray := C.OGR_F_GetFieldAsDoubleList(f.handle, fid, &length)
			fld.val = cDoubleArrayToSlice(cArray, length)
		case C.OFTStringList:
			fld.ftype = FTStringList
			cArray := C.OGR_F_GetFieldAsStringList(f.handle, fid)
			fld.val = cStringArrayToSlice(cArray)
		case C.OFTBinary:
			fld.ftype = FTBinary
			var length C.int
			cArray := C.OGR_F_GetFieldAsBinary(f.handle, fid, &length)
			var slice []byte
			if cArray != nil {
				slice = C.GoBytes(unsafe.Pointer(cArray), length)
			}
			fld.val = slice
		default:
			// Only deprecated field types like FTWideString & WideStringList should be handled by default case
			fld.ftype = FTUnknown
		}
		retm[fname] = fld
	}
	return retm
}

// Fetch field as date and time
func (f *Feature) getFieldAsDateTime(index C.int) *time.Time {
	var year, month, day, hour, minute, second, tzFlag int
	ret := C.OGR_F_GetFieldAsDateTime(
		f.handle,
		index,
		(*C.int)(unsafe.Pointer(&year)),
		(*C.int)(unsafe.Pointer(&month)),
		(*C.int)(unsafe.Pointer(&day)),
		(*C.int)(unsafe.Pointer(&hour)),
		(*C.int)(unsafe.Pointer(&minute)),
		(*C.int)(unsafe.Pointer(&second)),
		(*C.int)(unsafe.Pointer(&tzFlag)),
	)
	if ret != 0 {
		var location *time.Location
		// 0=unknown, 1=localtime, 100=GMT, 101=GMT+15minute, 99=GMT-15minute...
		switch tzFlag {
		case 0:
			location = &time.Location{}
		case 1:
			location = time.Local
		default:
			location = time.FixedZone(fmt.Sprintf("zone_%d", tzFlag), (tzFlag-100)*15*60)
		}
		t := time.Date(year, time.Month(month), day, hour, minute, second, 0, location)
		return &t
	}
	return nil
}

// Close releases resources associated to a feature
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

// CreateFeature creates a feature on Layer
func (layer Layer) CreateFeature(feat *Feature, opts ...CreateFeatureOption) error {
	cfo := createFeatureOpts{}
	for _, opt := range opts {
		opt.setCreateFeatureOpt(&cfo)
	}
	cgc := createCGOContext(nil, cfo.errorHandler)
	C.godalLayerCreateFeature(cgc.cPointer(), layer.handle(), feat.handle)
	if err := cgc.close(); err != nil {
		return err
	}
	return nil
}

// NewFeature creates a feature on Layer from a geometry
func (layer Layer) NewFeature(geom *Geometry, opts ...NewFeatureOption) (*Feature, error) {
	nfo := newFeatureOpts{}
	for _, opt := range opts {
		opt.setNewFeatureOpt(&nfo)
	}
	ghandle := C.OGRGeometryH(nil)
	if geom != nil {
		ghandle = geom.handle
	}
	cgc := createCGOContext(nil, nfo.errorHandler)
	hndl := C.godalLayerNewFeature(cgc.cPointer(), layer.handle(), ghandle)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Feature{hndl}, nil
}

// UpdateFeature rewrites an updated feature in the Layer
func (layer Layer) UpdateFeature(feat *Feature, opts ...UpdateFeatureOption) error {
	uo := &updateFeatureOpts{}
	for _, o := range opts {
		o.setUpdateFeatureOpt(uo)
	}
	cgc := createCGOContext(nil, uo.errorHandler)
	C.godalLayerSetFeature(cgc.cPointer(), layer.handle(), feat.handle)
	return cgc.close()
}

// DeleteFeature deletes feature from the Layer.
func (layer Layer) DeleteFeature(feat *Feature, opts ...DeleteFeatureOption) error {
	do := &deleteFeatureOpts{}
	for _, o := range opts {
		o.setDeleteFeatureOpt(do)
	}
	cgc := createCGOContext(nil, do.errorHandler)
	C.godalLayerDeleteFeature(cgc.cPointer(), layer.handle(), feat.handle)
	return cgc.close()
}

// SetGeometryColumnName set the name of feature first geometry field.
// Only supported when running with GDAL 3.6+.
func (layer Layer) SetGeometryColumnName(name string, opts ...SetGeometryColumnNameOption) error {
	so := &setGeometryColumnNameOpts{}
	for _, o := range opts {
		o.setGeometryColumnNameOpt(so)
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	cgc := createCGOContext(nil, so.errorHandler)
	C.godalLayerSetGeometryColumnName(cgc.cPointer(), layer.handle(), (*C.char)(cname))
	return cgc.close()
}

// CreateLayer creates a new vector layer
//
// Available CreateLayerOptions are
//   - FieldDefinition (may be used multiple times) to add attribute fields to the layer
func (ds *Dataset) CreateLayer(name string, sr *SpatialRef, gtype GeometryType, opts ...CreateLayerOption) (Layer, error) {
	co := createLayerOpts{}
	for _, opt := range opts {
		opt.setCreateLayerOpt(&co)
	}
	srHandle := C.OGRSpatialReferenceH(nil)
	if sr != nil {
		srHandle = sr.handle
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	cgc := createCGOContext(nil, co.errorHandler)
	hndl := C.godalCreateLayer(cgc.cPointer(), ds.handle(), (*C.char)(unsafe.Pointer(cname)), srHandle, C.OGRwkbGeometryType(gtype))
	if err := cgc.close(); err != nil {
		return Layer{}, err
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

// CopyLayer Duplicate an existing layer.
func (ds *Dataset) CopyLayer(source Layer, name string, opts ...CopyLayerOption) (Layer, error) {
	co := copyLayerOpts{}
	for _, opt := range opts {
		opt.setCopyLayerOpt(&co)
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	cgc := createCGOContext(nil, co.errorHandler)
	hndl := C.godalCopyLayer(cgc.cPointer(), ds.handle(), source.handle(), (*C.char)(unsafe.Pointer(cname)))
	if err := cgc.close(); err != nil {
		return Layer{}, err
	}
	return Layer{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// LayerByName fetch a layer by name. Returns nil if not found.
func (ds *Dataset) LayerByName(name string) *Layer {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	hndl := C.GDALDatasetGetLayerByName(ds.handle(), (*C.char)(unsafe.Pointer(cname)))
	if hndl == nil {
		return nil
	}
	return &Layer{majorObject{C.GDALMajorObjectH(hndl)}}
}

// ResultSet is a Layer generated by Dataset.ExecuteSQL
type ResultSet struct {
	Layer
	ds     *Dataset
	closed bool
}

// ExecuteSQL executes an SQL statement against the data store.
// This function may return a nil ResultSet when the SQL statement does not generate any rows to
// return (INSERT/UPDATE/DELETE/CREATE TABLE etc.)
func (ds *Dataset) ExecuteSQL(sql string, opts ...ExecuteSQLOption) (*ResultSet, error) {

	eso := executeSQLOpts{}
	for _, opt := range opts {
		opt.setExecuteSQLOpt(&eso)
	}

	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	cDialect := C.CString(string(eso.dialect))
	defer C.free(unsafe.Pointer(cDialect))

	if eso.dialect == "" {
		cDialect = nil
	}

	g := eso.spatialFilter.geom

	if g == nil {
		g = &Geometry{}
	}
	cgc := createCGOContext(nil, eso.errorHandler)
	hndl := C.godalDatasetExecuteSQL(cgc.cPointer(), ds.handle(), (*C.char)(unsafe.Pointer(csql)), g.handle, (*C.char)(unsafe.Pointer(cDialect)))

	if err := cgc.close(); err != nil {
		return nil, err
	}

	layer := Layer{majorObject{C.GDALMajorObjectH(hndl)}}

	return &ResultSet{layer, ds, false}, nil
}

// Close releases results of Dataset.ExecuteSQL
func (rs *ResultSet) Close(opts ...CloseResultSetOption) error {
	if rs.closed {
		return nil
	}

	crso := closeResultSetOpts{}
	for _, opt := range opts {
		opt.setReleaseResultSetOpt(&crso)
	}
	cgc := createCGOContext(nil, crso.errorHandler)
	C.godalReleaseResultSet(cgc.cPointer(), rs.ds.handle(), rs.handle())
	err := cgc.close()
	rs.closed = true
	return err
}

// StartTransaction creates a transaction for datasets which support transactions
func (ds *Dataset) StartTransaction(opts ...StartTransactionOption) error {

	sto := startTransactionOpts{}
	for _, opt := range opts {
		opt.setStartTransactionOpt(&sto)
	}

	cEff := C.int(0)

	if sto.bForce == EmulatedTx() {
		cEff = C.int(1)
	}

	cgc := createCGOContext(nil, sto.errorHandler)
	C.godalStartTransaction(cgc.cPointer(), ds.handle(), cEff)
	err := cgc.close()
	return err
}

// RollbackTransaction rolls back a Dataset to its state before the start of the current transaction
func (ds *Dataset) RollbackTransaction(opts ...RollbackTransactionOption) error {

	rto := rollbackTransactionOpts{}
	for _, opt := range opts {
		opt.setRollbackTransactionOpt(&rto)
	}

	cgc := createCGOContext(nil, rto.errorHandler)
	C.godalDatasetRollbackTransaction(cgc.cPointer(), ds.handle())
	err := cgc.close()
	return err
}

// CommitTransaction commits a transaction for a Dataset that supports transactions
func (ds *Dataset) CommitTransaction(opts ...CommitTransactionOption) error {

	cto := commitTransactionOpts{}
	for _, opt := range opts {
		opt.setCommitTransactionOpt(&cto)
	}

	cgc := createCGOContext(nil, cto.errorHandler)
	C.godalCommitTransaction(cgc.cPointer(), ds.handle())
	err := cgc.close()
	return err
}

// NewGeometryFromGeoJSON creates a new Geometry from its GeoJSON representation
func NewGeometryFromGeoJSON(geoJSON string, opts ...NewGeometryOption) (*Geometry, error) {
	no := &newGeometryOpts{}
	for _, o := range opts {
		o.setNewGeometryOpt(no)
	}

	cgeoJSON := C.CString(geoJSON)
	defer C.free(unsafe.Pointer(cgeoJSON))
	cgc := createCGOContext(nil, no.errorHandler)
	hndl := C.godalNewGeometryFromGeoJSON(cgc.cPointer(), (*C.char)(unsafe.Pointer(cgeoJSON)))
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{isOwned: true, handle: hndl}, nil
}

// NewGeometryFromWKT creates a new Geometry from its WKT representation
func NewGeometryFromWKT(wkt string, sr *SpatialRef, opts ...NewGeometryOption) (*Geometry, error) {
	no := &newGeometryOpts{}
	for _, o := range opts {
		o.setNewGeometryOpt(no)
	}
	srHandle := C.OGRSpatialReferenceH(nil)
	if sr != nil {
		srHandle = sr.handle
	}
	cwkt := C.CString(wkt)
	defer C.free(unsafe.Pointer(cwkt))
	cgc := createCGOContext(nil, no.errorHandler)
	hndl := C.godalNewGeometryFromWKT(cgc.cPointer(), (*C.char)(unsafe.Pointer(cwkt)), srHandle)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{isOwned: true, handle: hndl}, nil
}

// NewGeometryFromWKB creates a new Geometry from its WKB representation
func NewGeometryFromWKB(wkb []byte, sr *SpatialRef, opts ...NewGeometryOption) (*Geometry, error) {
	no := &newGeometryOpts{}
	for _, o := range opts {
		o.setNewGeometryOpt(no)
	}
	srHandle := C.OGRSpatialReferenceH(nil)
	if sr != nil {
		srHandle = sr.handle
	}
	cgc := createCGOContext(nil, no.errorHandler)
	hndl := C.godalNewGeometryFromWKB(cgc.cPointer(), unsafe.Pointer(&wkb[0]), C.int(len(wkb)), srHandle)
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Geometry{isOwned: true, handle: hndl}, nil
}

// WKT returns the Geomtry's WKT representation
func (g *Geometry) WKT(opts ...GeometryWKTOption) (string, error) {
	wo := &geometryWKTOpts{}
	for _, o := range opts {
		o.setGeometryWKTOpt(wo)
	}
	cgc := createCGOContext(nil, wo.errorHandler)
	cwkt := C.godalExportGeometryWKT(cgc.cPointer(), g.handle)
	if err := cgc.close(); err != nil {
		return "", err
	}
	wkt := C.GoString(cwkt)
	C.CPLFree(unsafe.Pointer(cwkt))
	return wkt, nil
}

// WKB returns the Geomtry's WKB representation
func (g *Geometry) WKB(opts ...GeometryWKBOption) ([]byte, error) {
	wo := &geometryWKBOpts{}
	for _, o := range opts {
		o.setGeometryWKBOpt(wo)
	}
	var cwkb unsafe.Pointer
	clen := C.int(0)
	cgc := createCGOContext(nil, wo.errorHandler)
	C.godalExportGeometryWKB(cgc.cPointer(), &cwkb, &clen, g.handle)
	if err := cgc.close(); err != nil {
		return nil, err
	}
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
func (g *Geometry) Reproject(to *SpatialRef, opts ...GeometryReprojectOption) error {
	gr := &geometryReprojectOpts{}
	for _, o := range opts {
		o.setGeometryReprojectOpt(gr)
	}
	cgc := createCGOContext(nil, gr.errorHandler)
	C.godalGeometryTransformTo(cgc.cPointer(), g.handle, to.handle)
	return cgc.close()
}

// Transform transforms the given geometry. g is expected to already be
// in the supplied Transform source SpatialRef.
func (g *Geometry) Transform(trn *Transform, opts ...GeometryTransformOption) error {
	gt := &geometryTransformOpts{}
	for _, o := range opts {
		o.setGeometryTransformOpt(gt)
	}
	cgc := createCGOContext(nil, gt.errorHandler)
	C.godalGeometryTransform(cgc.cPointer(), g.handle, trn.handle, trn.dst)
	return cgc.close()
}

// GeoJSON returns the geometry in geojson format. The geometry is expected to be in epsg:4326
// projection per RFCxxx
//
// Available GeoJSONOptions are
//   - SignificantDigits(n int) to keep n significant digits after the decimal separator (default: 8)
func (g *Geometry) GeoJSON(opts ...GeoJSONOption) (string, error) {
	gjo := geojsonOpts{
		precision: 7,
	}
	for _, opt := range opts {
		opt.setGeojsonOpt(&gjo)
	}
	cgc := createCGOContext(nil, gjo.errorHandler)
	gjdata := C.godalExportGeometryGeoJSON(cgc.cPointer(), g.handle, C.int(gjo.precision))
	if err := cgc.close(); err != nil {
		return "", err
	}
	wkt := C.GoString(gjdata)
	C.CPLFree(unsafe.Pointer(gjdata))
	return wkt, nil
}

// GML returns the geometry in GML format.
// See the GDAL exportToGML doc page to determine the GML conversion options that can be set through CreationOption.
//
// Example of conversion options :
//
//	g.GML(CreationOption("FORMAT=GML3","GML3_LONGSRS=YES"))
func (g *Geometry) GML(opts ...GMLExportOption) (string, error) {
	gmlo := &gmlExportOpts{}
	for _, o := range opts {
		o.setGMLExportOpt(gmlo)
	}
	cswitches := sliceToCStringArray(gmlo.creation)
	defer cswitches.free()
	cgc := createCGOContext(nil, gmlo.errorHandler)
	cgml := C.godalExportGeometryGML(cgc.cPointer(), g.handle, cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return "", err
	}
	gml := C.GoString(cgml)
	C.CPLFree(unsafe.Pointer(cgml))
	return gml, nil
}

// VSIFile is a handler around gdal's vsi handlers
type VSIFile struct {
	handle *C.VSILFILE
}

// VSIOpen opens path. path can be virtual, eg beginning with /vsimem/
func VSIOpen(path string, opts ...VSIOpenOption) (*VSIFile, error) {
	vo := &vsiOpenOpts{}
	for _, o := range opts {
		o.setVSIOpenOpt(vo)
	}
	cname := unsafe.Pointer(C.CString(path))
	defer C.free(cname)
	cgc := createCGOContext(nil, vo.errorHandler)
	hndl := C.godalVSIOpen(cgc.cPointer(), (*C.char)(cname))
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &VSIFile{hndl}, nil
}

// Close closes the VSIFile. Must be called exactly once.
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

// VSIUnlink deletes path
func VSIUnlink(path string, opts ...VSIUnlinkOption) error {
	vo := &vsiUnlinkOpts{}
	for _, o := range opts {
		o.setVSIUnlinkOpt(vo)
	}
	cname := unsafe.Pointer(C.CString(path))
	defer C.free(cname)
	cgc := createCGOContext(nil, vo.errorHandler)
	C.godalVSIUnlink(cgc.cPointer(), (*C.char)(cname))
	return cgc.close()
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

// KeySizerReaderAt is the interface expected when calling RegisterVSIHandler
//
// ReadAt() is a standard io.ReaderAt that takes a key (i.e. filename) as argument.
//
// Size() is used as a probe to determine wether the given key exists, and should return
// an error if no such key exists. The actual file size may or may not be effectively used
// depending on the underlying GDAL driver opening the file
//
// It may also optionally implement KeyMultiReader which will be used (only?) by
// the GTiff driver when reading pixels. If not provided, this
// VSI implementation will concurrently call ReadAt([]byte,int64)
type KeySizerReaderAt interface {
	ReadAt(key string, buf []byte, off int64) (int, error)
	Size(key string) (int64, error)
}

// KeyMultiReader is an optional interface that can be implemented by KeyReaderAtSizer that
// will be used (only?) by the GTiff driver when reading pixels. If not provided, this
// VSI implementation will concurrently call ReadAt(key,[]byte,int64)
type KeyMultiReader interface {
	ReadAtMulti(key string, bufs [][]byte, offs []int64) ([]int, error)
}

//export _gogdalSizeCallback
func _gogdalSizeCallback(ckey *C.char, errorString **C.char) C.longlong {
	key := C.GoString(ckey)
	cbd, err := getGoGDALReader(key)
	if err != nil {
		*errorString = C.CString(err.Error())
		return -1
	}

	if cbd.prefix > 0 {
		key = key[cbd.prefix:]
	}
	l, err := cbd.Size(key)
	if err != nil {
		*errorString = C.CString(err.Error())
	}
	return C.longlong(l)
}

//export _gogdalMultiReadCallback
func _gogdalMultiReadCallback(ckey *C.char, nRanges C.int, pocbuffers unsafe.Pointer, coffsets unsafe.Pointer, clengths unsafe.Pointer, errorString **C.char) C.int {
	key := C.GoString(ckey)
	cbd, err := getGoGDALReader(key)
	if err != nil {
		*errorString = C.CString(err.Error())
		return -1
	}
	/* cbd == nil would be a bug elsewhere */
	if cbd.prefix > 0 {
		key = key[cbd.prefix:]
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
	_, err = cbd.ReadAtMulti(key, buffers, goffsets)
	if err != nil && err != io.EOF {
		*errorString = C.CString(err.Error())
		ret = -1
	}
	return C.int(ret)
}

//export _gogdalReadCallback
func _gogdalReadCallback(ckey *C.char, buffer unsafe.Pointer, off C.size_t, clen C.size_t, errorString **C.char) C.size_t {
	l := int(clen)
	key := C.GoString(ckey)
	cbd, err := getGoGDALReader(key)
	if err != nil {
		*errorString = C.CString(err.Error())
		return 0
	}
	if cbd.prefix > 0 {
		key = key[cbd.prefix:]
	}
	slice := (*[1 << 28]byte)(buffer)[:l:l]
	rlen, err := cbd.ReadAt(key, slice, int64(off))
	if err != nil && err != io.EOF {
		*errorString = C.CString(err.Error())
	}
	return C.size_t(rlen)
}

var handlers map[string]vsiHandler

func getGoGDALReader(key string) (vsiHandler, error) {
	for prefix, handler := range handlers {
		if strings.HasPrefix(key, prefix) {
			return handler, nil
		}
	}
	return vsiHandler{}, fmt.Errorf("no handler registered")
}

type vsiHandler struct {
	KeySizerReaderAt
	prefix int
}

func (sp vsiHandler) ReadAtMulti(key string, bufs [][]byte, offs []int64) ([]int, error) {
	if mcbd, ok := sp.KeySizerReaderAt.(KeyMultiReader); ok {
		return mcbd.ReadAtMulti(key, bufs, offs)
	}
	var wg sync.WaitGroup
	wg.Add(len(bufs))
	lens := make([]int, len(bufs))
	var err error
	var errmu sync.Mutex
	for b := range bufs {
		go func(bidx int) {
			var berr error
			defer wg.Done()
			lens[bidx], berr = sp.ReadAt(key, bufs[bidx], offs[bidx])
			if berr != nil && berr != io.EOF {
				errmu.Lock()
				if err == nil {
					err = berr
				}
				errmu.Unlock()
			}
			if lens[bidx] != int(len(bufs[bidx])) {
				errmu.Lock()
				if err == nil {
					if berr != nil {
						err = berr
					} else {
						err = fmt.Errorf("short read")
					}
				}
				errmu.Unlock()
			}
		}(b)
	}
	wg.Wait()
	return lens, err
}

// RegisterVSIHandler registers an osio.Adapter on the given prefix.
// When registering an adapter with
//
//	RegisterVSIHandler("scheme://",handler)
//
// calling Open("scheme://myfile.txt") will result in godal making calls to
//
//	adapter.Reader("myfile.txt").ReadAt(buf,offset)
func RegisterVSIHandler(prefix string, handler KeySizerReaderAt, opts ...VSIHandlerOption) error {
	opt := vsiHandlerOpts{
		bufferSize:  64 * 1024,
		cacheSize:   2 * 64 * 1024,
		stripPrefix: false,
	}
	for _, o := range opts {
		o.setVSIHandlerOpt(&opt)
	}
	if handlers == nil {
		handlers = make(map[string]vsiHandler)
	}
	if _, ok := handlers[prefix]; ok {
		return fmt.Errorf("handler already registered on prefix")
	}
	cgc := createCGOContext(nil, opt.errorHandler)
	C.godalVSIInstallGoHandler(cgc.cPointer(), C.CString(prefix), C.size_t(opt.bufferSize), C.size_t(opt.cacheSize))
	if err := cgc.close(); err != nil {
		return err
	}
	if opt.stripPrefix {
		handlers[prefix] = vsiHandler{handler, len(prefix)}
	} else {
		handlers[prefix] = vsiHandler{handler, 0}
	}
	return nil
}

// HasVSIHandler returns true if a VSIHandler is registered for this prefix
func HasVSIHandler(prefix string) bool {
	return C.godalVSIHasGoHandler(C.CString(prefix)) != 0
}

// BuildVRT runs the GDALBuildVRT function and creates a VRT dataset from a list of datasets
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

	cname := unsafe.Pointer(C.CString(dstVRTName))
	defer C.free(cname)

	csources := sliceToCStringArray(sourceDatasets)
	defer csources.free()

	cgc := createCGOContext(bvo.config, bvo.errorHandler)
	hndl := C.godalBuildVRT(cgc.cPointer(), (*C.char)(cname), csources.cPointer(),
		cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return nil, err
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// GridCreate, creates a grid from scattered data, given provided gridding parameters as a string (pszAlgorithm)
// and the arguments required for `godalGridCreate()` (binding for GDALGridCreate)
//
// NOTE: For valid gridding algorithm strings see: https://gdal.org/programs/gdal_grid.html#interpolation-algorithms
func GridCreate(pszAlgorithm string,
	xCoords []float64,
	yCoords []float64,
	zCoords []float64,
	dfXMin float64,
	dfXMax float64,
	dfYMin float64,
	dfYMax float64,
	nXSize int,
	nYSize int,
	buffer interface{},
	opts ...GridCreateOption,
) error {
	if len(xCoords) != len(yCoords) || len(yCoords) != len(zCoords) {
		return errors.New("`xCoords`, `yCoords` and `zCoords` are not all equal length")
	}

	gco := gridCreateOpts{}
	for _, o := range opts {
		o.setGridCreateOpt(&gco)
	}

	griddingAlgStr := strings.Split(pszAlgorithm, ":")[0]
	algCEnum, err := gridAlgFromString(griddingAlgStr)
	if err != nil {
		return err
	}

	var (
		params = unsafe.Pointer(C.CString(pszAlgorithm))
		cgc    = createCGOContext(nil, gco.errorHandler)
	)
	defer C.free(params)

	var (
		dtype        = bufferType(buffer)
		dsize        = dtype.Size()
		numGridBytes = C.int(nXSize * nYSize * dsize)
		cBuf         = cBuffer(buffer, int(numGridBytes)/dsize)
	)
	cgc = createCGOContext(nil, gco.errorHandler)
	C.godalGridCreate(cgc.cPointer(), (*C.char)(params), algCEnum, C.uint(len(xCoords)), cDoubleArray(xCoords), cDoubleArray(yCoords), cDoubleArray(zCoords), C.double(dfXMin), C.double(dfXMax), C.double(dfYMin), C.double(dfYMax), C.uint(nXSize), C.uint(nYSize), C.GDALDataType(dtype), cBuf)
	if err := cgc.close(); err != nil {
		return err
	}
	return nil
}

// Grid runs the library version of gdal_grid.
// See the gdal_grid doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//
//	[]string{"-a", "maximum", "-txe", "0", "1"}
//
// Creation options and driver may be set in the switches slice with
//
//	switches:=[]string{"-co","TILED=YES","-of","GTiff"}
//
// NOTE: Some switches are NOT compatible with this binding, as a `nullptr` is passed to a later call to
// `GDALGridOptionsNew()` (as the 2nd argument). Those switches are: "-oo", "-q", "-quiet"
func (ds *Dataset) Grid(destPath string, switches []string, opts ...GridOption) (*Dataset, error) {
	gridOpts := gridOpts{}
	for _, opt := range opts {
		opt.setGridOpt(&gridOpts)
	}

	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()

	dest := unsafe.Pointer(C.CString(destPath))
	cgc := createCGOContext(nil, gridOpts.errorHandler)
	var dsRet C.GDALDatasetH
	defer C.free(unsafe.Pointer(dest))

	dsRet = C.godalGrid(cgc.cPointer(), (*C.char)(dest), ds.handle(), cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return nil, err
	}

	return &Dataset{majorObject{C.GDALMajorObjectH(dsRet)}}, nil
}

// Dem runs the library version of gdaldem.
// See the gdaldem doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches (for "hillshade", switches differ per mode):
//
//	[]string{"-s", "111120", "-alt", "45"}
//
// Creation options and driver may be set in the switches slice with
//
//	switches:=[]string{"-co","TILED=YES","-of","GTiff"}
//
// NOTE: `colorFilename` is a "text-based color configuration file" that MUST ONLY be
// provided when `processingMode` == "color-relief"
func (ds *Dataset) Dem(destPath, processingMode string, colorFilename string, switches []string, opts ...DemOption) (*Dataset, error) {
	demOpts := demOpts{}
	for _, opt := range opts {
		opt.setDemOpt(&demOpts)
	}

	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()

	dest := unsafe.Pointer(C.CString(destPath))
	defer C.free(unsafe.Pointer(dest))
	alg := unsafe.Pointer(C.CString(processingMode))
	defer C.free(unsafe.Pointer(alg))
	var colorFn *C.char
	if colorFilename != "" {
		colorFn = C.CString(colorFilename)
		defer C.free(unsafe.Pointer(colorFn))
	}

	cgc := createCGOContext(nil, demOpts.errorHandler)
	dsRet := C.godalDem(cgc.cPointer(), (*C.char)(dest), (*C.char)(alg), colorFn, ds.handle(), cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return nil, err
	}

	return &Dataset{majorObject{C.GDALMajorObjectH(dsRet)}}, nil
}

// ViewshedMode is the "cell height calculation mode" for the viewshed process
//
// Source: https://github.com/OSGeo/gdal/blob/master/alg/viewshed/viewshed_types.h
type ViewshedMode uint32

const (
	// MDiagonal is the "diagonal mode"
	MDiagonal ViewshedMode = iota + 1
	// MEdge is the "edge mode"
	MEdge
	// MMax is the "maximum value produced by Diagonal and Edge mode"
	MMax
	// MMin is the "minimum value produced by Diagonal and Edge mode"
	MMin
)

// ViewshedOutputType sets the return type and information represented by the returned data
//
// Source: https://gdal.org/en/stable/programs/gdal_viewshed.html
//
// NOTE: "Cumulative (ACCUM)" mode not currently supported, as it's not available in the `GDALViewshedGenerate` function
// (it's only used in the command line invocation of `viewshed`)
type ViewshedOutputType uint32

const (
	// Normal returns a raster of type Byte containing visible locations
	Normal ViewshedOutputType = iota + 1
	// MinTargetHeightFromDem return a raster of type Float64 containing the minimum target height for target to be visible from the DEM surface
	MinTargetHeightFromDem
	// MinTargetHeightFromGround return a raster of type Float64 containing the minimum target height for target to be visible from ground level
	MinTargetHeightFromGround
)

// Viewshed (binding for GDALViewshedGenerate), creates a viewshed from a raster DEM, these parameters (mostly) map to to parameters for GDALViewshedGenerate
//
// for more information see: https://gdal.org/en/stable/api/gdal_alg.html#_CPPv420GDALViewshedGenerate15GDALRasterBandHPKcPKc12CSLConstListddddddddd16GDALViewshedModed16GDALProgressFuncPv22GDALViewshedOutputType12CSLConstList
//
// Creations options can be set through options with:
//
//	Viewshed(bnd, "mem", "none", ..., CreationOption("TILED=YES","BLOCKXSIZE=256"))
func Viewshed(targetBand Band, driverName *DriverName, targetRasterName string, observerX float64, observerY float64, observerHeight float64, targetHeight float64,
	visibleVal float64, invisibleVal float64, outOfRangeVal float64, noDataVal float64, curveCoeff float64, mode ViewshedMode, maxDistance float64,
	heightMode ViewshedOutputType, opts ...ViewshedOption) (*Dataset, error) {

	// TODO: Should I put a 'warning' in the documentation for `Viewshed` instead of disallowing the last two configurations?
	if !CheckMinVersion(3, 1, 0) {
		return nil, errors.New("failed to run, 'viewshed' not supported on GDAL versions < 3.1.0")
	} else if !CheckMinVersion(3, 4, 2) {
		return nil, errors.New("cannot run 'viewshed' with GDAL version <= 3.4.1, as some tests produce invalid results under these conditions")
	} else if !CheckMinVersion(3, 10, 0) && heightMode == MinTargetHeightFromDem {
		return nil, errors.New("height mode CANNOT be `MinTargetHeightFromDem` when running a GDAL version < 3.10, as some tests produce invalid results under these conditions")
	}

	// Allow `driverName` to be null and handle it here to match parameter/behaviour of GDALViewshedGenerate
	defaultDriverName := GTiff
	if driverName == nil {
		driverName = &defaultDriverName
	}

	viewshedOpts := viewshedOpts{}
	for _, opt := range opts {
		opt.setViewshedOpt(&viewshedOpts)
	}

	copts := sliceToCStringArray(viewshedOpts.creation)
	defer copts.free()
	driver := unsafe.Pointer(C.CString(string(*driverName)))
	defer C.free(unsafe.Pointer(driver))
	targetRaster := unsafe.Pointer(C.CString(targetRasterName))
	defer C.free(unsafe.Pointer(targetRaster))

	cgc := createCGOContext(nil, viewshedOpts.errorHandler)
	dsRet := C.godalViewshedGenerate(cgc.cPointer(), targetBand.handle(), (*C.char)(driver), (*C.char)(targetRaster), copts.cPointer(), C.double(observerX),
		C.double(observerY), C.double(observerHeight), C.double(targetHeight), C.double(visibleVal), C.double(invisibleVal), C.double(outOfRangeVal),
		C.double(noDataVal), C.double(curveCoeff), C.uint(mode), C.double(maxDistance), C.uint(heightMode))
	if err := cgc.close(); err != nil {
		return nil, err
	}

	return &Dataset{majorObject{C.GDALMajorObjectH(dsRet)}}, nil
}

// Nearblack runs the library version of nearblack
//
// See the nearblack doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//
//	[]string{"-white", "-near", "10"}
//
// Creation options and driver may be set in the switches slice with
//
//	switches:=[]string{"-co","TILED=YES","-of","GTiff"}
//
// NOTE: Some switches are NOT compatible with this binding, as a `nullptr` is passed to a later call to
// `GDALNearblackOptionsNew()` (as the 2nd argument). Those switches are: "-o", "-q", "-quiet"
func (ds *Dataset) Nearblack(dstDS string, switches []string, opts ...NearblackOption) (*Dataset, error) {
	nearBlackOpts := nearBlackOpts{}
	for _, opt := range opts {
		opt.setNearblackOpt(&nearBlackOpts)
	}

	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()

	dest := unsafe.Pointer(C.CString(dstDS))
	defer C.free(dest)

	cgc := createCGOContext(nil, nearBlackOpts.errorHandler)

	ret := C.godalNearblack(cgc.cPointer(), (*C.char)(dest), nil, ds.handle(), cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return nil, err
	}

	return &Dataset{majorObject{C.GDALMajorObjectH(ret)}}, nil
}

// NearblackInto writes the provided `sourceDs` into the Dataset that this method was called on, and
// runs the library version of nearblack.
//
// See the nearblack doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//
//	[]string{"-white", "-near", "10"}
//
// Creation options and driver may be set in the switches slice with
//
//	switches:=[]string{"-co","TILED=YES","-of","GTiff"}
//
// NOTE: Some switches are NOT compatible with this binding, as a `nullptr` is passed to a later call to
// `GDALNearblackOptionsNew()` (as the 2nd argument). Those switches are: "-o", "-q", "-quiet"
func (ds *Dataset) NearblackInto(sourceDs *Dataset, switches []string, opts ...NearblackOption) error {
	nearBlackOpts := nearBlackOpts{}
	for _, opt := range opts {
		opt.setNearblackOpt(&nearBlackOpts)
	}

	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()

	cgc := createCGOContext(nil, nearBlackOpts.errorHandler)

	var srcDsHandle C.GDALDatasetH = nil
	if sourceDs != nil {
		srcDsHandle = sourceDs.handle()
	}
	_ = C.godalNearblack(cgc.cPointer(), nil, ds.handle(), srcDsHandle, cswitches.cPointer())
	if err := cgc.close(); err != nil {
		return err
	}

	return nil
}

// GCP mirrors the structure of the GDAL_GCP type
type GCP struct {
	PszId      string
	PszInfo    string
	DfGCPPixel float64
	DfGCPLine  float64
	DfGCPX     float64
	DfGCPY     float64
	DfGCPZ     float64
}

// gdalGCPToGoGCPArray is a utility function for conversion from `C.GCPsAndCount` (GDAL) to `[]GCP` (Go)
func gdalGCPToGoGCPArray(gcp C.GCPsAndCount) []GCP {
	var ret []GCP
	if gcp.gcpList == nil {
		return ret
	}

	//https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
	gcps := (*[1 << 30]C.GDAL_GCP)(unsafe.Pointer(gcp.gcpList))
	ret = make([]GCP, gcp.numGCPs)
	for i := 0; i < len(ret); i++ {
		ret[i] = GCP{
			PszId:      C.GoString(gcps[i].pszId),
			PszInfo:    C.GoString(gcps[i].pszInfo),
			DfGCPPixel: float64(gcps[i].dfGCPPixel),
			DfGCPLine:  float64(gcps[i].dfGCPLine),
			DfGCPX:     float64(gcps[i].dfGCPX),
			DfGCPY:     float64(gcps[i].dfGCPY),
			DfGCPZ:     float64(gcps[i].dfGCPZ),
		}
	}

	return ret
}

// GetGCPSpatialRef runs the GDALGetGCPSpatialRef function
func (ds *Dataset) GCPSpatialRef() *SpatialRef {
	return &SpatialRef{handle: C.godalGetGCPSpatialRef(ds.handle()), isOwned: false}
}

// GetGCPs runs the GDALGetGCPs function
func (ds *Dataset) GCPs() []GCP {
	gcpsAndCount := C.godalGetGCPs(ds.handle())
	return gdalGCPToGoGCPArray(gcpsAndCount)
}

// GetGCPProjection runs the GDALGetGCPProjection function
func (ds *Dataset) GCPProjection() string {
	return C.GoString(C.godalGetGCPProjection(ds.handle()))
}

// SetGCPs runs the GDALSetGCPs function
func (ds *Dataset) SetGCPs(GCPList []GCP, opts ...SetGCPsOption) error {
	setGCPsOpts := setGCPsOpts{}
	for _, opt := range opts {
		opt.setSetGCPsOpt(&setGCPsOpts)
	}

	// Convert `[]GCP` -> `C.goGCPList`
	var gcpList C.goGCPList
	var (
		ids       = make([]string, len(GCPList))
		infos     = make([]string, len(GCPList))
		gcpPixels = make([]float64, len(GCPList))
		gcpLines  = make([]float64, len(GCPList))
		gcpXs     = make([]float64, len(GCPList))
		gcpYs     = make([]float64, len(GCPList))
		gcpZs     = make([]float64, len(GCPList))
	)
	for i, g := range GCPList {
		ids[i] = g.PszId
		infos[i] = g.PszInfo
		gcpPixels[i] = (g.DfGCPPixel)
		gcpLines[i] = (g.DfGCPLine)
		gcpXs[i] = (g.DfGCPX)
		gcpYs[i] = (g.DfGCPY)
		gcpZs[i] = (g.DfGCPZ)
	}
	cIds := sliceToCStringArray(ids)
	defer cIds.free()
	cInfos := sliceToCStringArray(infos)
	defer cInfos.free()

	gcpList.pszIds = cIds.cPointer()
	gcpList.pszInfos = cInfos.cPointer()
	gcpList.dfGCPPixels = cDoubleArray(gcpPixels)
	gcpList.dfGCPLines = cDoubleArray(gcpLines)
	gcpList.dfGCPXs = cDoubleArray(gcpXs)
	gcpList.dfGCPYs = cDoubleArray(gcpYs)
	gcpList.dfGCPZs = cDoubleArray(gcpZs)

	cgc := createCGOContext(nil, setGCPsOpts.errorHandler)
	if setGCPsOpts.sr != nil {
		C.godalSetGCPs2(cgc.cPointer(), ds.handle(), C.int(len(GCPList)), gcpList, setGCPsOpts.sr.handle)
	} else {
		GCPProj := C.CString(setGCPsOpts.projString)
		defer C.free(unsafe.Pointer(GCPProj))
		C.godalSetGCPs(cgc.cPointer(), ds.handle(), C.int(len(GCPList)), gcpList, GCPProj)
	}

	if err := cgc.close(); err != nil {
		return err
	}
	return nil
}

// Convert list of GCPs to a GDAL GeoTransorm array
func GCPsToGeoTransform(GCPList []GCP, opts ...GCPsToGeoTransformOption) ([6]float64, error) {
	gco := gcpsToGeoTransformOpts{}
	for _, opt := range opts {
		opt.setGCPsToGeoTransformOpts(&gco)
	}

	// Convert `[]GCP` -> `C.goGCPList`
	var gcpList C.goGCPList
	var (
		ids       = make([]string, len(GCPList))
		infos     = make([]string, len(GCPList))
		gcpPixels = make([]float64, len(GCPList))
		gcpLines  = make([]float64, len(GCPList))
		gcpXs     = make([]float64, len(GCPList))
		gcpYs     = make([]float64, len(GCPList))
		gcpZs     = make([]float64, len(GCPList))
	)
	for i, g := range GCPList {
		ids[i] = g.PszId
		infos[i] = g.PszInfo
		gcpPixels[i] = (g.DfGCPPixel)
		gcpLines[i] = (g.DfGCPLine)
		gcpXs[i] = (g.DfGCPX)
		gcpYs[i] = (g.DfGCPY)
		gcpZs[i] = (g.DfGCPZ)
	}
	cIds := sliceToCStringArray(ids)
	defer cIds.free()
	cInfos := sliceToCStringArray(infos)
	defer cInfos.free()

	gcpList.pszIds = cIds.cPointer()
	gcpList.pszInfos = cInfos.cPointer()
	gcpList.dfGCPPixels = cDoubleArray(gcpPixels)
	gcpList.dfGCPLines = cDoubleArray(gcpLines)
	gcpList.dfGCPXs = cDoubleArray(gcpXs)
	gcpList.dfGCPYs = cDoubleArray(gcpYs)
	gcpList.dfGCPZs = cDoubleArray(gcpZs)

	gt := make([]C.double, 6)
	cgt := (*C.double)(unsafe.Pointer(&gt[0]))
	ret := [6]float64{}
	var cgc = createCGOContext(nil, gco.errorHandler)
	C.godalGCPListToGeoTransform(cgc.cPointer(), gcpList, C.int(len(GCPList)), cgt)
	if err := cgc.close(); err != nil {
		return ret, err
	}

	// Copy the values from the C Array into a Go array
	for i := range gt {
		ret[i] = float64(gt[i])
	}

	return ret, nil
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

// frees the context and returns any error it may contain
func (cgc cgoContext) close() error {
	cgc.opts.free()
	defer C.free(unsafe.Pointer(cgc.cctx))
	if cgc.cctx.errMessage != nil {
		/* debug code
		if cgc.cctx.handlerIdx != 0 {
			panic("bug!")
		}
		*/
		defer C.free(unsafe.Pointer(cgc.cctx.errMessage))
		return errors.New(C.GoString(cgc.cctx.errMessage))
	}

	if cgc.cctx.handlerIdx != 0 {
		defer unregisterErrorHandler(int(cgc.cctx.handlerIdx))
		return getErrorHandler(int(cgc.cctx.handlerIdx)).err
	}
	return nil
}
