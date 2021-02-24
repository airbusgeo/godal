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
	"strconv"
	"unsafe"
)

type majorObject struct {
	handle C.GDALMajorObjectH
}

//Dataset is a wrapper around a GDALDatasetH
type Dataset struct {
	majorObject
}

//Handle returns a pointer to the underlying GDALDatasetH
func (ds *Dataset) Handle() C.GDALDatasetH {
	return C.GDALDatasetH(ds.majorObject.handle)
}

type openOptions struct {
	flags        uint
	drivers      []string //list of drivers that can be tried to open the given name
	options      []string //driver specific open options (see gdal docs for each driver)
	siblingFiles []string //list of sidecar files
	config       []string
}

//OpenOption is an option passed to Open()
//
// Available OpenOptions are:
//
// • Drivers
//
// • SiblingFiles
//
// • Shared
//
// • ConfigOption
//
// • Update
//
// • DriverOpenOption
//
// • RasterOnly
//
// • VectorOnly
type OpenOption interface {
	setOpenOption(oo *openOptions)
}

//Open calls GDALOpenEx() with the provided options. It returns nil and an error
//in case there was an error opening the provided dataset name.
//name may be a filename or any supported string supported by gdal (e.g. a /vsixxx path,
//the xml string representing a vrt dataset, etc...)
func Open(name string, options ...OpenOption) (*Dataset, error) {
	oopts := openOptions{
		flags: C.GDAL_OF_READONLY | C.GDAL_OF_VERBOSE_ERROR,
	}
	for _, opt := range options {
		opt.setOpenOption(&oopts)
	}
	csiblings := sliceToCStringArray(oopts.siblingFiles)
	coopts := sliceToCStringArray(oopts.options)
	cdrivers := sliceToCStringArray(oopts.drivers)
	cconfig := sliceToCStringArray(oopts.config)
	defer csiblings.free()
	defer coopts.free()
	defer cdrivers.free()
	defer cconfig.free()
	var errmsg *C.char
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	retds := C.godalOpen(cname, C.uint(oopts.flags),
		cdrivers.cPointer(), coopts.cPointer(), csiblings.cPointer(),
		(**C.char)(unsafe.Pointer(&errmsg)), cconfig.cPointer())

	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		if retds != nil {
			C.GDALClose(retds)
		}
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(retds)}}, nil
}

//Close releases the dataset
func (ds *Dataset) Close() error {
	if ds.handle == nil {
		return fmt.Errorf("close called more than once")
	}
	var errmsg *C.char
	C.godalClose(ds.Handle(), (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	ds.handle = nil
	return nil
}

// Block is a window inside a dataset, starting at pixel X0,Y0 and spanning
// W,H pixels.
type Block struct {
	X0, Y0 int
	W, H   int
	bw, bh int //block size
	sx, sy int //img size
	nx, ny int //num blocks
	i, j   int //cur
}

// Next returns the following block in scanline order. It returns Block{},false
// when there are no more blocks in the scanlines
func (b Block) Next() (Block, bool) {
	nb := b
	nb.i++
	if nb.i >= nb.nx {
		nb.i = 0
		nb.j++
	}
	if nb.j >= nb.ny {
		return Block{}, false
	}
	nb.X0 = nb.i * nb.bw
	nb.Y0 = nb.j * nb.bh
	nb.W, nb.H = actualBlockSize(nb.sx, nb.sy, nb.bw, nb.bh, nb.i, nb.j)

	return nb, true
}

// BlockIterator returns the blocks covering a sizeX,sizeY dataset.
// All sizes must be strictly positive.
func BlockIterator(sizeX, sizeY int, blockSizeX, blockSizeY int) Block {
	bl := Block{
		X0: 0,
		Y0: 0,
		i:  0,
		j:  0,
		bw: blockSizeX,
		bh: blockSizeY,
		sx: sizeX,
		sy: sizeY,
	}
	bl.nx, bl.ny = (sizeX+blockSizeX-1)/blockSizeX,
		(sizeY+blockSizeY-1)/blockSizeY
	bl.W, bl.H = actualBlockSize(sizeX, sizeY, blockSizeX, blockSizeY, 0, 0)
	return bl
}

// BandStructure implements Structure for a Band
type BandStructure struct {
	SizeX, SizeY           int
	BlockSizeX, BlockSizeY int
	DataType               DataType
}

// DatasetStructure implements Structure for a Dataset
type DatasetStructure struct {
	BandStructure
	NBands int
}

// FirstBlock returns the topleft block definition
func (is BandStructure) FirstBlock() Block {
	return BlockIterator(is.SizeX, is.SizeY, is.BlockSizeX, is.BlockSizeY)
}

// BlockCount returns the number of blocks in the x and y dimensions
func (is BandStructure) BlockCount() (int, int) {
	return (is.SizeX + is.BlockSizeX - 1) / is.BlockSizeX,
		(is.SizeY + is.BlockSizeY - 1) / is.BlockSizeY
}

// ActualBlockSize returns the number of pixels in the x and y dimensions
// that actually contain data for the given x,y block
func (is BandStructure) ActualBlockSize(blockX, blockY int) (int, int) {
	return actualBlockSize(is.SizeX, is.SizeY, is.BlockSizeX, is.BlockSizeY, blockX, blockY)
}

func actualBlockSize(sizeX, sizeY int, blockSizeX, blockSizeY int, blockX, blockY int) (int, int) {
	cx, cy := (sizeX+blockSizeX-1)/blockSizeX,
		(sizeY+blockSizeY-1)/blockSizeY
	if blockX < 0 || blockY < 0 || blockX >= cx || blockY >= cy {
		return 0, 0
	}
	retx := blockSizeX
	rety := blockSizeY
	if blockX == cx-1 {
		nXPixelOff := blockX * blockSizeX
		retx = sizeX - nXPixelOff
	}
	if blockY == cy-1 {
		nYPixelOff := blockY * blockSizeY
		rety = sizeY - nYPixelOff
	}
	return retx, rety
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

type buildVRTOpts struct {
	config      []string
	openOptions []string
	bands       []int
	resampling  ResamplingAlg
}

// BuildVRTOption is an option that can be passed to BuildVRT
//
// Available BuildVRTOptions are:
//
// • ConfigOption
//
// • DriverOpenOption
//
// • Bands
//
// • Resampling
type BuildVRTOption interface {
	setBuildVRTOpt(bvo *buildVRTOpts)
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
