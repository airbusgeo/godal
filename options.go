package godal

import "sort"

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
//
// • PixelSpacing
//
// • LineSpacing
type BandIOOption interface {
	setBandIOOpt(ro *bandIOOpt)
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

type dsCreateOpts struct {
	config   []string
	creation []string
}

// DatasetCreateOption is an option that can be passed to Create()
//
// Available DatasetCreateOptions are:
//
// • CreationOption
//
// • ConfigOption
type DatasetCreateOption interface {
	setDatasetCreateOpt(dc *dsCreateOpts)
}

type openOptions struct {
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
	setOpenOption(oo *openOptions)
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
func (sf siblingFilesOpt) setOpenOption(oo *openOptions) {
	if len(sf.files) > 0 {
		oo.siblingFiles = append(oo.siblingFiles, sf.files...)
	} else {
		oo.siblingFiles = nil
	}
}

type metadataOpt struct {
	domain string
}

// MetadataOption is an option that can be passed to metadata related calls
// Available MetadataOptions are:
//
// • Domain
type MetadataOption interface {
	setMetadataOpt(mo *metadataOpt)
}

// Domain specifies the gdal metadata domain to use
func Domain(mdDomain string) interface {
	MetadataOption
} {
	return metadataOpt{mdDomain}
}
func (mdo metadataOpt) setMetadataOpt(mo *metadataOpt) {
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

func (bo bandOpt) setDatasetIOOpt(ro *datasetIOOpt) {
	ro.bands = bo.bnds
}
func (bo bandOpt) setBuildOverviewsOpt(ovr *buildOvrOpts) {
	ovr.bands = bo.bnds
}
func (bo bandOpt) setRasterizeGeometryOpt(o *rasterizeGeometryOpt) {
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
	BuildVRTOption
	errorAndLoggingOption
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
func (co configOpts) setBuildVRTOpt(bvo *buildVRTOpts) {
	bvo.config = append(bvo.config, co.config...)
}
func (co configOpts) setErrorAndLoggingOpt(elo *errorAndLoggingOpts) {
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
func (ro resamplingOpt) setDatasetIOOpt(io *datasetIOOpt) {
	io.resampling = ro.m
}
func (ro resamplingOpt) setBandIOOpt(io *bandIOOpt) {
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

type rasterizeGeometryOpt struct {
	bands      []int
	values     []float64
	allTouched int
}

// RasterizeGeometryOption is an option that can be passed tp Dataset.RasterizeGeometry()
type RasterizeGeometryOption interface {
	setRasterizeGeometryOpt(o *rasterizeGeometryOpt)
}

type allTouchedOpt struct{}

func (at allTouchedOpt) setRasterizeGeometryOpt(o *rasterizeGeometryOpt) {
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
	config   []string
	creation []string
	driver   DriverName
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

type newFeatureOpt struct{}

//NewFeatureOption is an option that can be passed to Layer.NewFeature
//
// Available options are:
//
// • none yet
type NewFeatureOption interface {
	setNewFeatureOpt(nfo *newFeatureOpt)
}

type createLayerOpts struct {
	fields []*FieldDefinition
}

// CreateLayerOption is an option that can be passed to Dataset.CreateLayer()
type CreateLayerOption interface {
	setCreateLayerOpt(clo *createLayerOpts)
}

type geojsonOpt struct {
	precision int
}

//GeoJSONOption is an option that can be passed to Geometry.GeoJSON
type GeoJSONOption interface {
	setGeojsonOpt(gjo *geojsonOpt)
}

type significantDigits int

func (sd significantDigits) setGeojsonOpt(o *geojsonOpt) {
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
