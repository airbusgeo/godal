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

// Handle returns a pointer to the underlying GDALRasterBandH
func (band Band) Handle() C.GDALRasterBandH {
	return C.GDALRasterBandH(band.majorObject.handle)
}

// Structure returns the dataset's Structure
func (band Band) Structure() BandStructure {
	var sx, sy, bsx, bsy, dtype C.int
	C.godalBandStructure(band.Handle(), &sx, &sy, &bsx, &bsy, &dtype)
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
	cn := C.GDALGetRasterNoDataValue(band.Handle(), &cok)
	if cok != 0 {
		return float64(cn), true
	}
	return 0, false
}

//SetNoData sets the band's nodata value
func (band Band) SetNoData(nd float64) error {
	errmsg := C.godalSetRasterNoDataValue(band.Handle(), C.double(nd))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// ClearNoData clears the band's nodata value
func (band Band) ClearNoData() error {
	errmsg := C.godalDeleteRasterNoDataValue(band.Handle())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// ColorInterp returns the band's color interpretation (defaults to Gray)
func (band Band) ColorInterp() ColorInterp {
	colorInterp := C.GDALGetRasterColorInterpretation(band.Handle())
	return ColorInterp(colorInterp)
}

// SetColorInterp sets the band's color interpretation
func (band Band) SetColorInterp(colorInterp ColorInterp) error {
	errmsg := C.godalSetRasterColorInterpretation(band.Handle(), C.GDALColorInterp(colorInterp))
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
	return int(C.GDALGetMaskFlags(band.Handle()))
}

//MaskBand returns the mask (nodata) band for this band. May be generated from nodata values.
func (band Band) MaskBand() Band {
	hndl := C.GDALGetMaskBand(band.Handle())
	return Band{majorObject{C.GDALMajorObjectH(hndl)}}
}

type bandCreateMaskOpts struct {
	config []string
}

// BandCreateMaskOption is an option that can be passed to Band.CreateMask()
//
// Available BandCreateMaskOptions are:
//
// • ConfigOption
type BandCreateMaskOption interface {
	setBandCreateMaskOpt(dcm *bandCreateMaskOpts)
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
	hndl := C.godalCreateMaskBand(band.Handle(), C.int(flags), &errmsg, cconfig.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return Band{}, errors.New(C.GoString(errmsg))
	}
	return Band{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

//Fill sets the whole band uniformely to (real,imag)
func (band Band) Fill(real, imag float64) error {
	errmsg := C.godalFillRaster(band.Handle(), C.double(real), C.double(imag))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

type bandIOOpt struct {
	config                    []string
	dsWidth, dsHeight         int
	resampling                ResamplingAlg
	pixelSpacing, lineSpacing int
}

// BandIOOption is an option to modify the default behavior of band.IO
//
// Available BandIOOptions are:
//
// • Stride
//
// • Window
//
// • Resampling
//
// • ConfigOption
type BandIOOption interface {
	setBandIOOpt(ro *bandIOOpt)
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

	errmsg := C.godalBandRasterIO(band.Handle(), C.GDALRWFlag(rw),
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

type polygonizeOpt struct {
	mask          *Band
	options       []string
	pixFieldIndex int
}

// PolygonizeOption is an option to modify the default behavior of band.IO
//
// Available PolygonizeOptions are:
//
// • EightConnected() to enable 8-connectivity. Leave out completely for 4-connectivity (default)
//
// • PixelValueFieldIndex(fieldidx) to populate the fieldidx'th field of the output
// dataset with the polygon's pixel value
//
// • Mask(band) to use given band as nodata mask instead of the internal nodata mask
type PolygonizeOption interface {
	setPolygonizeOpt(ro *polygonizeOpt)
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
		cMaskBand = popt.mask.Handle()
	}

	errmsg := C.godalPolygonize(band.Handle(), cMaskBand, dstLayer.Handle(), C.int(popt.pixFieldIndex), copts.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

//Overviews returns all overviews of band
func (band Band) Overviews() []Band {
	cbands := C.godalBandOverviews(band.Handle())
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

// Histogram is a band's histogram.
type Histogram struct {
	min, max float64
	counts   []uint64
}

// Bucket is a histogram entry. It spans [Min,Max] and contains Count entries.
type Bucket struct {
	Min, Max float64
	Count    uint64
}

//Len returns the number of buckets contained in the histogram
func (h Histogram) Len() int {
	return len(h.counts)
}

//Bucket returns the i'th bucket in the histogram. i must be between 0 and Len()-1.
func (h Histogram) Bucket(i int) Bucket {
	width := (h.max - h.min) / float64(len(h.counts))
	return Bucket{
		Min:   h.min + width*float64(i),
		Max:   h.min + width*float64(i+1),
		Count: h.counts[i],
	}
}

type histogramOpts struct {
	approx         C.int
	includeOutside C.int
	min, max       C.double
	buckets        C.int
}

// HistogramOption is an option that can be passed to Band.Histogram()
//
// Available HistogramOptions are:
//
// • Approximate() to allow the algorithm to operate on a subset of the full resolution data
//
// • Intervals(count int, min,max float64) to compute a histogram with count buckets, spanning [min,max].
//   Each bucket will be (max-min)/count wide. If not provided, the default histogram will be returned.
//
// • IncludeOutOfRange() to populate the first and last bucket with values under/over the specified min/max
//   when used in conjuntion with Intervals()
type HistogramOption interface {
	setHistogramOpt(ho *histogramOpts)
}

type includeOutsideOpt struct{}

func (ioo includeOutsideOpt) setHistogramOpt(ho *histogramOpts) {
	ho.includeOutside = C.int(1)
}

// IncludeOutOfRange populates the first and last bucket with values under/over the specified min/max
// when used in conjuntion with Intervals()
func IncludeOutOfRange() interface {
	HistogramOption
} {
	return includeOutsideOpt{}
}

type approximateOkOption struct{}

func (aoo approximateOkOption) setHistogramOpt(ho *histogramOpts) {
	ho.approx = C.int(1)
}

// Approximate allows the histogram algorithm to operate on a subset of the full resolution data
func Approximate() interface {
	HistogramOption
} {
	return approximateOkOption{}
}

type intervalsOption struct {
	min, max float64
	buckets  int
}

func (io intervalsOption) setHistogramOpt(ho *histogramOpts) {
	ho.min = C.double(io.min)
	ho.max = C.double(io.max)
	ho.buckets = C.int(io.buckets)
}

// Intervals computes a histogram with count buckets, spanning [min,max].
// Each bucket will be (max-min)/count wide. If not provided, the default histogram will be returned.
func Intervals(count int, min, max float64) interface {
	HistogramOption
} {
	return intervalsOption{min: min, max: max, buckets: count}
}

//Histogram returns or computes the bands histogram
func (band Band) Histogram(opts ...HistogramOption) (Histogram, error) {
	hopt := histogramOpts{}
	for _, o := range opts {
		o.setHistogramOpt(&hopt)
	}
	var values *C.ulonglong = nil
	defer C.VSIFree(unsafe.Pointer(values))

	errmsg := C.godalRasterHistogram(band.Handle(), &hopt.min, &hopt.max, &hopt.buckets,
		&values, hopt.includeOutside, hopt.approx)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return Histogram{}, errors.New(C.GoString(errmsg))
	}
	counts := (*[1 << 30]C.ulonglong)(unsafe.Pointer(values))
	h := Histogram{
		min:    float64(hopt.min),
		max:    float64(hopt.max),
		counts: make([]uint64, int(hopt.buckets)),
	}
	for i := 0; i < int(hopt.buckets); i++ {
		h.counts[i] = uint64(counts[i])
	}
	return h, nil
}
