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

import "sort"

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

type siblingFilesOpt struct {
	files []string
}

//SiblingFiles specifies the list of files that may be opened alongside the prinicpal dataset name.
//This should only be used on dataset names that are backed by an actual filesystem: see gdal docs
//
//files must not contain a directory component (i.e. are expected to be in the same directory as
//the main dataset)
func SiblingFiles(files ...string) interface {
	OpenOption
} {
	return siblingFilesOpt{files}
}
func (sf siblingFilesOpt) setOpenOption(oo *openOptions) {
	oo.siblingFiles = append(oo.siblingFiles, sf.files...)
}

type driversOpt struct {
	drivers []string
}

//Drivers specifies the list of drivers that are allowed to try opening the dataset
func Drivers(drivers ...string) interface {
	OpenOption
} {
	return driversOpt{drivers}
}
func (do driversOpt) setOpenOption(oo *openOptions) {
	oo.drivers = append(oo.drivers, do.drivers...)
}

type driverOpenOption struct {
	oo []string
}

//DriverOpenOption adds a list of Open Options (-oo switch) to the open command. Each keyval must
//be provided in a "KEY=value" format
func DriverOpenOption(keyval ...string) interface {
	OpenOption
} {
	return driverOpenOption{keyval}
}
func (doo driverOpenOption) setOpenOption(oo *openOptions) {
	oo.options = append(oo.options, doo.oo...)
}

type bandOpt struct {
	bnds []int
}

// Bands specifies which dataset bands should be read/written. By default all dataset bands
// are read/written.
//
// Note: bnds is 0-indexed so as to be consistent with Dataset.Bands(), whereas in GDAL terminology,
// bands are 1-indexed. i.e. for a 3 band dataset you should pass Bands(0,1,2) and not Bands(1,2,3).
func Bands(bnds ...int) interface {
	DatasetIOOption
	BuildOverviewsOption
	RasterizeGeometryOption
} {
	ib := make([]int, len(bnds))
	for i := range bnds {
		ib[i] = bnds[i] + 1
	}
	return bandOpt{ib}
}

func (bo bandOpt) setDatasetIOOpt(ro *datasetIOOpt) {
	ro.bands = bo.bnds
}
func (bo bandOpt) setBuildOverviewsOpt(ovr *buildOvrOpts) {
	ovr.bands = bo.bnds
}
func (bo bandOpt) setRasterizeGeometryOpt(o *rasterizeGeometryOpt) {
	o.bands = bo.bnds
}

type bandSpacingOpt struct {
	sp int
}
type pixelSpacingOpt struct {
	sp int
}
type lineSpacingOpt struct {
	sp int
}

func (so bandSpacingOpt) setDatasetIOOpt(ro *datasetIOOpt) {
	ro.bandSpacing = so.sp
}
func (so pixelSpacingOpt) setDatasetIOOpt(ro *datasetIOOpt) {
	ro.pixelSpacing = so.sp
}
func (so lineSpacingOpt) setDatasetIOOpt(ro *datasetIOOpt) {
	ro.lineSpacing = so.sp
}
func (so lineSpacingOpt) setBandIOOpt(bo *bandIOOpt) {
	bo.lineSpacing = so.sp
}
func (so pixelSpacingOpt) setBandIOOpt(bo *bandIOOpt) {
	bo.pixelSpacing = so.sp
}

// BandSpacing sets the number of bytes from one pixel to the next band of the same pixel. If not
// provided, it will be calculated from the pixel type
func BandSpacing(stride int) interface {
	DatasetIOOption
} {
	return bandSpacingOpt{stride}
}

// PixelSpacing sets the number of bytes from one pixel to the next pixel in the same row. If not
// provided, it will be calculated from the number of bands and pixel type
func PixelSpacing(stride int) interface {
	DatasetIOOption
	BandIOOption
} {
	return pixelSpacingOpt{stride}
}

// LineSpacing sets the number of bytes from one pixel to the pixel of the same band one row below. If not
// provided, it will be calculated from the number of bands, pixel type and image width
func LineSpacing(stride int) interface {
	DatasetIOOption
	BandIOOption
} {
	return lineSpacingOpt{stride}
}

type windowOpt struct {
	sx, sy int
}

// Window specifies the size of the dataset window to read/write. By default use the
// size of the input/output buffer (i.e. no resampling)
func Window(sx, sy int) interface {
	DatasetIOOption
	BandIOOption
} {
	return windowOpt{sx, sy}
}

func (wo windowOpt) setDatasetIOOpt(ro *datasetIOOpt) {
	ro.dsWidth = wo.sx
	ro.dsHeight = wo.sy
}
func (wo windowOpt) setBandIOOpt(ro *bandIOOpt) {
	ro.dsWidth = wo.sx
	ro.dsHeight = wo.sy
}

type bandInterleaveOp struct{}

// BandInterleaved makes Read return a band interleaved buffer instead of a pixel interleaved one.
//
// For example, pixels of a three band RGB image will be returned in order
// r1r2r3...rn, g1g2g3...gn, b1b2b3...bn instead of the default
// r1g1b1, r2g2b2, r3g3b3, ... rnbngn
//
// BandInterleaved should not be used in conjunction with BandSpacing, LineSpacing, or PixelSpacing
func BandInterleaved() interface {
	DatasetIOOption
} {
	return bandInterleaveOp{}
}

func (bio bandInterleaveOp) setDatasetIOOpt(ro *datasetIOOpt) {
	ro.bandInterleave = true
}

type creationOpts struct {
	creation []string
}

// CreationOption are options to pass to a driver when creating a dataset, to be
// passed in the form KEY=VALUE
//
// Examples are: BLOCKXSIZE=256, COMPRESS=LZW, NUM_THREADS=8, etc...
func CreationOption(opts ...string) interface {
	DatasetCreateOption
	DatasetWarpOption
	DatasetTranslateOption
	DatasetVectorTranslateOption
	RasterizeOption
} {
	return creationOpts{opts}
}

func (co creationOpts) setDatasetCreateOpt(dc *dsCreateOpts) {
	dc.creation = append(dc.creation, co.creation...)
}
func (co creationOpts) setDatasetWarpOpt(dc *dsWarpOpts) {
	dc.creation = append(dc.creation, co.creation...)
}
func (co creationOpts) setDatasetTranslateOpt(dc *dsTranslateOpts) {
	dc.creation = append(dc.creation, co.creation...)
}
func (co creationOpts) setDatasetVectorTranslateOpt(dc *dsVectorTranslateOpts) {
	dc.creation = append(dc.creation, co.creation...)
}
func (co creationOpts) setRasterizeOpt(o *rasterizeOpts) {
	o.create = append(o.create, co.creation...)
}

type configOpts struct {
	config []string
}

// ConfigOption sets a configuration option for a gdal library call. See the
// specific gdal function doc page and specific driver docs for allowed values.
//
// Notable options are GDAL_NUM_THREADS=8
func ConfigOption(cfgs ...string) interface {
	BuildOverviewsOption
	DatasetCreateOption
	DatasetWarpOption
	DatasetWarpIntoOption
	DatasetTranslateOption
	DatasetCreateMaskOption
	DatasetVectorTranslateOption
	BandCreateMaskOption
	OpenOption
	RasterizeOption
	DatasetIOOption
	BandIOOption
} {
	return configOpts{cfgs}
}

func (co configOpts) setBuildOverviewsOpt(bo *buildOvrOpts) {
	bo.config = append(bo.config, co.config...)
}
func (co configOpts) setDatasetCreateOpt(dc *dsCreateOpts) {
	dc.config = append(dc.config, co.config...)
}
func (co configOpts) setDatasetWarpOpt(dc *dsWarpOpts) {
	dc.config = append(dc.config, co.config...)
}
func (co configOpts) setDatasetWarpIntoOpt(dc *dsWarpIntoOpts) {
	dc.config = append(dc.config, co.config...)
}
func (co configOpts) setDatasetTranslateOpt(dc *dsTranslateOpts) {
	dc.config = append(dc.config, co.config...)
}
func (co configOpts) setDatasetVectorTranslateOpt(dc *dsVectorTranslateOpts) {
	dc.config = append(dc.config, co.config...)
}
func (co configOpts) setDatasetCreateMaskOpt(dcm *dsCreateMaskOpts) {
	dcm.config = append(dcm.config, co.config...)
}
func (co configOpts) setBandCreateMaskOpt(bcm *bandCreateMaskOpts) {
	bcm.config = append(bcm.config, co.config...)
}
func (co configOpts) setOpenOption(oo *openOptions) {
	oo.config = append(oo.config, co.config...)
}
func (co configOpts) setRasterizeOpt(oo *rasterizeOpts) {
	oo.config = append(oo.config, co.config...)
}
func (co configOpts) setDatasetIOOpt(oo *datasetIOOpt) {
	oo.config = append(oo.config, co.config...)
}
func (co configOpts) setBandIOOpt(oo *bandIOOpt) {
	oo.config = append(oo.config, co.config...)
}

type minSizeOpt struct {
	s int
}

// MinSize makes BuildOverviews automatically compute the overview levels
// until the smallest overview size is less than s.
//
// Should not be used together with Levels()
func MinSize(s int) interface {
	BuildOverviewsOption
} {
	return minSizeOpt{s}
}

func (ms minSizeOpt) setBuildOverviewsOpt(bo *buildOvrOpts) {
	bo.minSize = ms.s
}

type resamplingOpt struct {
	m ResamplingAlg
}

//Resampling defines the resampling algorithm to use.
//If unset will usually default to NEAREST. See gdal docs for which algorithms are
//available.
func Resampling(alg ResamplingAlg) interface {
	BuildOverviewsOption
	DatasetIOOption
	BandIOOption
} {
	return resamplingOpt{alg}
}
func (ro resamplingOpt) setBuildOverviewsOpt(bo *buildOvrOpts) {
	bo.resampling = ro.m
}
func (ro resamplingOpt) setDatasetIOOpt(io *datasetIOOpt) {
	io.resampling = ro.m
}
func (ro resamplingOpt) setBandIOOpt(io *bandIOOpt) {
	io.resampling = ro.m
}

type levelsOpt struct {
	lvl []int
}

// Levels set the overview levels to be computed. This is usually:
//  Levels(2,4,8,16,32)
func Levels(levels ...int) interface {
	BuildOverviewsOption
} {
	return levelsOpt{levels}
}
func (lo levelsOpt) setBuildOverviewsOpt(bo *buildOvrOpts) {
	slevels := make([]int, len(lo.lvl))
	copy(slevels, lo.lvl)
	sort.Slice(slevels, func(i, j int) bool {
		return slevels[i] < slevels[j]
	})
	bo.levels = slevels
}

type maskBandOpt struct {
	band *Band
}

func (mbo maskBandOpt) setPolygonizeOpt(o *polygonizeOpt) {
	o.mask = mbo.band
}

// Mask makes Polygonize use the given band as a nodata mask
// instead of using the source band's nodata mask
func Mask(band Band) interface {
	PolygonizeOption
} {
	return maskBandOpt{&band}
}

// NoMask makes Polygonize ignore band nodata mask
func NoMask() interface {
	PolygonizeOption
} {
	return maskBandOpt{}
}

type polyPixField struct {
	fld int
}

func (ppf polyPixField) setPolygonizeOpt(o *polygonizeOpt) {
	o.pixFieldIndex = ppf.fld
}

// PixelValueFieldIndex makes Polygonize write the polygon's pixel
// value into the layer's fld'th field
func PixelValueFieldIndex(fld int) interface {
	PolygonizeOption
} {
	return polyPixField{fld}
}

type eightConnected struct{}

func (ec eightConnected) setPolygonizeOpt(o *polygonizeOpt) {
	o.options = append(o.options, "8CONNECTED=YES")
}

//EightConnected is an option that switches pixel connectivity from 4 to 8
func EightConnected() interface {
	PolygonizeOption
} {
	return eightConnected{}
}

type floatValues struct {
	v []float64
}

func (v floatValues) setRasterizeGeometryOpt(o *rasterizeGeometryOpt) {
	o.values = v.v
}

// Values sets the value(s) that must be rasterized in the dataset bands.
// vals must either be a single value that will be applied to all bands, or
// exactly match the number of requested bands
func Values(vals ...float64) interface {
	RasterizeGeometryOption
} {
	return floatValues{vals}
}

func (sr *SpatialRef) setBoundsOpt(o *boundsOpt) {
	o.sr = sr
}
