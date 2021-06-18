package godal

import "sort"

//GetGeoTransformOption is an option that can be passed to Dataset.GeoTransform()
//
// Available GetGeoTransformOptions are:
//
// • ErrLogger
type GetGeoTransformOption interface {
	setGetGeoTransformOpt(ndo *getGeoTransformOpts)
}
type getGeoTransformOpts struct {
	errorHandler ErrorHandler
}

//SetGeoTransformOption is an option that can be passed to Dataset.SetGeoTransform()
//
// Available SetGeoTransformOptions are:
//
// • ErrLogger
type SetGeoTransformOption interface {
	setSetGeoTransformOpt(ndo *setGeoTransformOpts)
}
type setGeoTransformOpts struct {
	errorHandler ErrorHandler
}

//SetProjectionOption is an option that can be passed to Dataset.SetProjection
//
// Available SetProjection are:
//
// • ErrLogger
type SetProjectionOption interface {
	setSetProjectionOpt(ndo *setProjectionOpts)
}
type setProjectionOpts struct {
	errorHandler ErrorHandler
}

//SetSpatialRefOption is an option that can be passed to Dataset.SetSpatialRef
//
// Available SetProjection are:
//
// • ErrLogger
type SetSpatialRefOption interface {
	setSetSpatialRefOpt(ndo *setSpatialRefOpts)
}
type setSpatialRefOpts struct {
	errorHandler ErrorHandler
}

//SetNoDataOption is an option that can be passed to Band.SetNodata(),
//Band.ClearNodata(), Dataset.SetNodata()
//
// Available SetNoDataOptions are:
//
// • ErrLogger
type SetNoDataOption interface {
	setSetNoDataOpt(ndo *setNodataOpts)
}
type setNodataOpts struct {
	errorHandler ErrorHandler
}

//SetColorInterpOption is an option that can be passed to Band.SetColorInterpretation()
//
// Available SetColorInterpOption are:
//
// • ErrLogger
type SetColorInterpOption interface {
	setSetColorInterpOpt(ndo *setColorInterpOpts)
}
type setColorInterpOpts struct {
	errorHandler ErrorHandler
}

//SetColorTableOption is an option that can be passed to Band.SetColorTable()
//
// Available SetColorTableOption are:
//
// • ErrLogger
type SetColorTableOption interface {
	setSetColorTableOpt(ndo *setColorTableOpts)
}
type setColorTableOpts struct {
	errorHandler ErrorHandler
}

type fillBandOpts struct {
	errorHandler ErrorHandler
}
type FillBandOption interface {
	setFillBandOpt(o *fillBandOpts)
}

type bandCreateMaskOpts struct {
	config       []string
	errorHandler ErrorHandler
}

// BandCreateMaskOption is an option that can be passed to Band.CreateMask()
//
// Available BandCreateMaskOptions are:
//
// • ConfigOption
//
// • ErrLogger
type BandCreateMaskOption interface {
	setBandCreateMaskOpt(dcm *bandCreateMaskOpts)
}

type bandIOOpts struct {
	config                    []string
	dsWidth, dsHeight         int
	resampling                ResamplingAlg
	pixelSpacing, lineSpacing int
	errorHandler              ErrorHandler
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
//
// • PixelSpacing
//
// • LineSpacing
type BandIOOption interface {
	setBandIOOpt(ro *bandIOOpts)
}

type fillnodataOpts struct {
	mask         *Band
	options      []string
	maxDistance  int
	iterations   int
	errorHandler ErrorHandler
}

// FillNoDataOption is an option that can be passed to band.FillNoData
//
// Available FillNoDataOptions are:
//
// • MaxDistance(int): The maximum distance (in pixels) that the algorithm will
// search out for values to interpolate. The default is 100 pixels.
//
// • SmoothIterations(int): The number of 3x3 average filter smoothing iterations
// to run after the interpolation to dampen artifacts. The default is zero smoothing iterations.
//
// • Mask(band) to use given band as nodata mask. The default uses the internal nodata mask
type FillNoDataOption interface {
	setFillnodataOpt(ro *fillnodataOpts)
}

type polygonizeOpts struct {
	mask          *Band
	options       []string
	pixFieldIndex int
	errorHandler  ErrorHandler
}

// PolygonizeOption is an option to modify the default behavior of band.Polygonize
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
	setPolygonizeOpt(ro *polygonizeOpts)
}

type dsCreateMaskOpts struct {
	config       []string
	errorHandler ErrorHandler
}

// DatasetCreateMaskOption is an option that can be passed to Dataset.CreateMaskBand()
//
// Available DatasetCreateMaskOptions are:
//
// • ConfigOption
type DatasetCreateMaskOption interface {
	setDatasetCreateMaskOpt(dcm *dsCreateMaskOpts)
}

type dsTranslateOpts struct {
	config       []string
	creation     []string
	driver       DriverName
	errorHandler ErrorHandler
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

type dsWarpOpts struct {
	config       []string
	creation     []string
	driver       DriverName
	errorHandler ErrorHandler
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
	config       []string
	errorHandler ErrorHandler
}

type buildOvrOpts struct {
	config       []string
	minSize      int
	resampling   ResamplingAlg
	bands        []int
	levels       []int
	errorHandler ErrorHandler
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
type clearOvrOpts struct {
	errorHandler ErrorHandler
}
type ClearOverviewsOption interface {
	setClearOverviewsOpt(bo *clearOvrOpts)
}

type datasetIOOpts struct {
	config                                 []string
	bands                                  []int
	dsWidth, dsHeight                      int
	resampling                             ResamplingAlg
	bandInterleave                         bool //return r1r2...rn,g1g2...gn,b1b2...bn instead of r1g1b1,r2g2b2,...,rngnbn
	bandSpacing, pixelSpacing, lineSpacing int
	errorHandler                           ErrorHandler
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
	setDatasetIOOpt(ro *datasetIOOpts)
}

type dsCreateOpts struct {
	config       []string
	creation     []string
	errorHandler ErrorHandler
}

// DatasetCreateOption is an option that can be passed to Create()
//
// Available DatasetCreateOptions are:
//
// • CreationOption
//
// • ConfigOption
//
// • ErrLogger
type DatasetCreateOption interface {
	setDatasetCreateOpt(dc *dsCreateOpts)
}

type openOpts struct {
	flags        uint
	drivers      []string //list of drivers that can be tried to open the given name
	options      []string //driver specific open options (see gdal docs for each driver)
	siblingFiles []string //list of sidecar files
	config       []string
	errorHandler ErrorHandler
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
	setOpenOpt(oo *openOpts)
}

type closeOpts struct {
	errorHandler ErrorHandler
}
type CloseOption interface {
	setCloseOpt(o *closeOpts)
}

type featureCountOpts struct {
	errorHandler ErrorHandler
}
type FeatureCountOption interface {
	setFeatureCountOpt(fo *featureCountOpts)
}

type simplifyOpts struct {
	errorHandler ErrorHandler
}
type bufferOpts struct {
	errorHandler ErrorHandler
}
type SimplifyOption interface {
	setSimplifyOpt(so *simplifyOpts)
}
type BufferOption interface {
	setBufferOpt(bo *bufferOpts)
}

type setGeometryOpts struct {
	errorHandler ErrorHandler
}
type SetGeometryOption interface {
	setSetGeometryOpt(so *setGeometryOpts)
}

type vsiOpenOpts struct {
	errorHandler ErrorHandler
}
type VSIOpenOption interface {
	setVSIOpenOpt(vo *vsiOpenOpts)
}
type vsiUnlinkOpts struct {
	errorHandler ErrorHandler
}
type VSIUnlinkOption interface {
	setVSIUnlinkOpt(vo *vsiUnlinkOpts)
}

type geometryWKTOpts struct {
	errorHandler ErrorHandler
}
type GeometryWKTOption interface {
	setGeometryWKTOpt(o *geometryWKTOpts)
}
type geometryWKBOpts struct {
	errorHandler ErrorHandler
}
type GeometryWKBOption interface {
	setGeometryWKBOpt(o *geometryWKBOpts)
}

type newGeometryOpts struct {
	errorHandler ErrorHandler
}
type NewGeometryOption interface {
	setNewGeometryOpt(o *newGeometryOpts)
}

type updateFeatureOpts struct {
	errorHandler ErrorHandler
}
type UpdateFeatureOption interface {
	setUpdateFeatureOpt(o *updateFeatureOpts)
}
type deleteFeatureOpts struct {
	errorHandler ErrorHandler
}
type DeleteFeatureOption interface {
	setDeleteFeatureOpt(o *deleteFeatureOpts)
}

type geometryTransformOpts struct {
	errorHandler ErrorHandler
}
type GeometryTransformOption interface {
	setGeometryTransformOpt(o *geometryTransformOpts)
}
type geometryReprojectOpts struct {
	errorHandler ErrorHandler
}
type GeometryReprojectOption interface {
	setGeometryReprojectOpt(o *geometryReprojectOpts)
}
type siblingFilesOpt struct {
	files []string
}

//SiblingFiles specifies the list of files that may be opened alongside the prinicpal dataset name.
//
//files must not contain a directory component (i.e. are expected to be in the same directory as
//the main dataset)
//
// SiblingFiles may be used in 3 different manners:
//
// • By default, i.e. by not using the option, godal will consider that there are no sibling files
// at all and will prevent any scanning or probing of specific sibling files by passing a list of
// sibling files to gdal containing only the main file
//
// • By passing a list of files, only those files will be probed
//
// • By passing SiblingFiles() (i.e. with an empty list of files), the default gdal behavior of
// reading the directory content and/or probing for well-known sidecar filenames will be used.
func SiblingFiles(files ...string) interface {
	OpenOption
} {
	return siblingFilesOpt{files}
}
func (sf siblingFilesOpt) setOpenOpt(oo *openOpts) {
	if len(sf.files) > 0 {
		oo.siblingFiles = append(oo.siblingFiles, sf.files...)
	} else {
		oo.siblingFiles = nil
	}
}

type metadataOpts struct {
	domain       string
	errorHandler ErrorHandler
}
type domainOpt struct {
	domain string
}

// MetadataOption is an option that can be passed to metadata related calls
// Available MetadataOptions are:
//
// • Domain
type MetadataOption interface {
	setMetadataOpt(mo *metadataOpts)
}

// Domain specifies the gdal metadata domain to use
func Domain(mdDomain string) interface {
	MetadataOption
} {
	return domainOpt{mdDomain}
}
func (mdo domainOpt) setMetadataOpt(mo *metadataOpts) {
	mo.domain = mdo.domain
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
	BuildVRTOption
} {
	ib := make([]int, len(bnds))
	for i := range bnds {
		ib[i] = bnds[i] + 1
	}
	return bandOpt{ib}
}

func (bo bandOpt) setDatasetIOOpt(ro *datasetIOOpts) {
	ro.bands = bo.bnds
}
func (bo bandOpt) setBuildOverviewsOpt(ovr *buildOvrOpts) {
	ovr.bands = bo.bnds
}
func (bo bandOpt) setRasterizeGeometryOpt(o *rasterizeGeometryOpts) {
	o.bands = bo.bnds
}
func (bo bandOpt) setBuildVRTOpt(bvo *buildVRTOpts) {
	bvo.bands = bo.bnds
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

func (so bandSpacingOpt) setDatasetIOOpt(ro *datasetIOOpts) {
	ro.bandSpacing = so.sp
}
func (so pixelSpacingOpt) setDatasetIOOpt(ro *datasetIOOpts) {
	ro.pixelSpacing = so.sp
}
func (so lineSpacingOpt) setDatasetIOOpt(ro *datasetIOOpts) {
	ro.lineSpacing = so.sp
}
func (so lineSpacingOpt) setBandIOOpt(bo *bandIOOpts) {
	bo.lineSpacing = so.sp
}
func (so pixelSpacingOpt) setBandIOOpt(bo *bandIOOpts) {
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

func (wo windowOpt) setDatasetIOOpt(ro *datasetIOOpts) {
	ro.dsWidth = wo.sx
	ro.dsHeight = wo.sy
}
func (wo windowOpt) setBandIOOpt(ro *bandIOOpts) {
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

func (bio bandInterleaveOp) setDatasetIOOpt(ro *datasetIOOpts) {
	ro.bandInterleave = true
}

type creationOpt struct {
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
	return creationOpt{opts}
}

func (co creationOpt) setDatasetCreateOpt(dc *dsCreateOpts) {
	dc.creation = append(dc.creation, co.creation...)
}
func (co creationOpt) setDatasetWarpOpt(dc *dsWarpOpts) {
	dc.creation = append(dc.creation, co.creation...)
}
func (co creationOpt) setDatasetTranslateOpt(dc *dsTranslateOpts) {
	dc.creation = append(dc.creation, co.creation...)
}
func (co creationOpt) setDatasetVectorTranslateOpt(dc *dsVectorTranslateOpts) {
	dc.creation = append(dc.creation, co.creation...)
}
func (co creationOpt) setRasterizeOpt(o *rasterizeOpts) {
	o.create = append(o.create, co.creation...)
}

type configOpt struct {
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
	BuildVRTOption
	errorAndLoggingOption
} {
	return configOpt{cfgs}
}

func (co configOpt) setBuildOverviewsOpt(bo *buildOvrOpts) {
	bo.config = append(bo.config, co.config...)
}
func (co configOpt) setDatasetCreateOpt(dc *dsCreateOpts) {
	dc.config = append(dc.config, co.config...)
}
func (co configOpt) setDatasetWarpOpt(dc *dsWarpOpts) {
	dc.config = append(dc.config, co.config...)
}
func (co configOpt) setDatasetWarpIntoOpt(dc *dsWarpIntoOpts) {
	dc.config = append(dc.config, co.config...)
}
func (co configOpt) setDatasetTranslateOpt(dc *dsTranslateOpts) {
	dc.config = append(dc.config, co.config...)
}
func (co configOpt) setDatasetVectorTranslateOpt(dc *dsVectorTranslateOpts) {
	dc.config = append(dc.config, co.config...)
}
func (co configOpt) setDatasetCreateMaskOpt(dcm *dsCreateMaskOpts) {
	dcm.config = append(dcm.config, co.config...)
}
func (co configOpt) setBandCreateMaskOpt(bcm *bandCreateMaskOpts) {
	bcm.config = append(bcm.config, co.config...)
}
func (co configOpt) setOpenOpt(oo *openOpts) {
	oo.config = append(oo.config, co.config...)
}
func (co configOpt) setRasterizeOpt(oo *rasterizeOpts) {
	oo.config = append(oo.config, co.config...)
}
func (co configOpt) setDatasetIOOpt(oo *datasetIOOpts) {
	oo.config = append(oo.config, co.config...)
}
func (co configOpt) setBandIOOpt(oo *bandIOOpts) {
	oo.config = append(oo.config, co.config...)
}
func (co configOpt) setBuildVRTOpt(bvo *buildVRTOpts) {
	bvo.config = append(bvo.config, co.config...)
}
func (co configOpt) setErrorAndLoggingOpt(elo *errorAndLoggingOpts) {
	elo.config = append(elo.config, co.config...)
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
	BuildVRTOption
} {
	return resamplingOpt{alg}
}
func (ro resamplingOpt) setBuildOverviewsOpt(bo *buildOvrOpts) {
	bo.resampling = ro.m
}
func (ro resamplingOpt) setDatasetIOOpt(io *datasetIOOpts) {
	io.resampling = ro.m
}
func (ro resamplingOpt) setBandIOOpt(io *bandIOOpts) {
	io.resampling = ro.m
}
func (ro resamplingOpt) setBuildVRTOpt(bvo *buildVRTOpts) {
	bvo.resampling = ro.m
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

func (mbo maskBandOpt) setPolygonizeOpt(o *polygonizeOpts) {
	o.mask = mbo.band
}
func (mbo maskBandOpt) setFillnodataOpt(o *fillnodataOpts) {
	o.mask = mbo.band
}

// Mask makes Polygonize or FillNoData use the given band as a nodata mask
// instead of using the source band's nodata mask
func Mask(band Band) interface {
	PolygonizeOption
	FillNoDataOption
} {
	return maskBandOpt{&band}
}

// NoMask makes Polygonize ignore band nodata mask
func NoMask() interface {
	PolygonizeOption
} {
	return maskBandOpt{}
}

type maxDistanceOpt struct {
	d float64
}

func (mdo maxDistanceOpt) setFillnodataOpt(o *fillnodataOpts) {
	o.maxDistance = int(mdo.d)
}

// MaxDistance is an option that can be passed to Band.FillNoData which sets the maximum number of
// pixels to search in all directions to find values to interpolate from.
func MaxDistance(d float64) interface {
	FillNoDataOption
} {
	return maxDistanceOpt{d}
}

type smoothingIterationsOpt struct {
	it int
}

func (sio smoothingIterationsOpt) setFillnodataOpt(o *fillnodataOpts) {
	o.iterations = sio.it
}

// SmoothingIterations is an option that can be passed to Band.FillNoData which sets the number of
// 3x3 smoothing filter passes to run (0 or more).
func SmoothingIterations(iterations int) interface {
	FillNoDataOption
} {
	return smoothingIterationsOpt{iterations}
}

type polyPixField struct {
	fld int
}

func (ppf polyPixField) setPolygonizeOpt(o *polygonizeOpts) {
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

func (ec eightConnected) setPolygonizeOpt(o *polygonizeOpts) {
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

func (v floatValues) setRasterizeGeometryOpt(o *rasterizeGeometryOpts) {
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

type rasterizeOpts struct {
	create       []string
	config       []string
	driver       DriverName
	errorHandler ErrorHandler
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

type rasterizeGeometryOpts struct {
	bands        []int
	values       []float64
	allTouched   int
	errorHandler ErrorHandler
}

// RasterizeGeometryOption is an option that can be passed tp Dataset.RasterizeGeometry()
type RasterizeGeometryOption interface {
	setRasterizeGeometryOpt(o *rasterizeGeometryOpts)
}

type allTouchedOpt struct{}

func (at allTouchedOpt) setRasterizeGeometryOpt(o *rasterizeGeometryOpts) {
	o.allTouched = 1
}

// AllTouched is an option that can be passed to Dataset.RasterizeGeometries()
// where all pixels touched by lines or polygons will be updated, not just those on the line
// render path, or whose center point is within the polygon.
func AllTouched() interface {
	RasterizeGeometryOption
} {
	return allTouchedOpt{}
}

type dsVectorTranslateOpts struct {
	config       []string
	creation     []string
	driver       DriverName
	errorHandler ErrorHandler
}

// DatasetVectorTranslateOption is an option that can be passed to Dataset.Warp()
//
// Available Options are:
//
// • CreationOption
// • ConfigOption
// • DriverName
type DatasetVectorTranslateOption interface {
	setDatasetVectorTranslateOpt(dwo *dsVectorTranslateOpts)
}

type newFeatureOpts struct {
	errorHandler ErrorHandler
}

//NewFeatureOption is an option that can be passed to Layer.NewFeature
//
// Available options are:
//
// • none yet
type NewFeatureOption interface {
	setNewFeatureOpt(nfo *newFeatureOpts)
}

type createLayerOpts struct {
	fields       []*FieldDefinition
	errorHandler ErrorHandler
}

// CreateLayerOption is an option that can be passed to Dataset.CreateLayer()
type CreateLayerOption interface {
	setCreateLayerOpt(clo *createLayerOpts)
}

type geojsonOpts struct {
	precision    int
	errorHandler ErrorHandler
}

//GeoJSONOption is an option that can be passed to Geometry.GeoJSON
type GeoJSONOption interface {
	setGeojsonOpt(gjo *geojsonOpts)
}

type significantDigits int

func (sd significantDigits) setGeojsonOpt(o *geojsonOpts) {
	o.precision = int(sd)
}

// SignificantDigits sets the number of significant digits after the decimal separator should
// be kept for geojson output
func SignificantDigits(n int) interface {
	GeoJSONOption
} {
	return significantDigits(n)
}

type buildVRTOpts struct {
	config       []string
	openOptions  []string
	bands        []int
	resampling   ResamplingAlg
	errorHandler ErrorHandler
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

type vsiHandlerOpts struct {
	bufferSize, cacheSize int
	errorHandler          ErrorHandler
}

// VSIHandlerOption is an option that can be passed to RegisterVSIHandler
type VSIHandlerOption interface {
	setVSIHandlerOpt(v *vsiHandlerOpts)
}

type bufferSizeOpt struct {
	b int
}

func (b bufferSizeOpt) setVSIHandlerOpt(v *vsiHandlerOpts) {
	v.bufferSize = b.b
}

type cacheSizeOpt struct {
	b int
}

func (b cacheSizeOpt) setVSIHandlerOpt(v *vsiHandlerOpts) {
	v.cacheSize = b.b
}

// VSIHandlerBufferSize sets the size of the gdal-native block size used for caching. Must be positive,
// can be set to 0 to disable this behavior (not recommended).
//
// Defaults to 64Kb
func VSIHandlerBufferSize(s int) VSIHandlerOption {
	return bufferSizeOpt{s}
}

// VSIHandlerCacheSize sets the total number of gdal-native bytes used as cache *per handle*.
// Defaults to 128Kb.
func VSIHandlerCacheSize(s int) VSIHandlerOption {
	return cacheSizeOpt{s}
}
