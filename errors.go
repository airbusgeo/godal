package godal

import "sync"

var errorHandlerMu sync.Mutex
var errorHandlerIndex int

// ErrorHandler is a function that can be used to override godal's default behavior
// of treating all messages with severity >= CE_Warning as errors. When an ErrorHandler
// is passed as an option to a godal function, all logs/errors emitted by gdal will be passed
// to this function, which can decide wether the parameters correspond to an actual error
// or not.
//
// If the ErrorHandler returns nil, the parent function will not return an error. It is up
// to the ErrorHandler to log the message if needed.
//
// If the ErrorHandler returns an error, that error will be returned as-is to the caller
// of the parent function
type ErrorHandler func(ec ErrorCategory, code int, msg string) error

type errorHandlerWrapper struct {
	fn     ErrorHandler
	errors []error
}

var errorHandlers = make(map[int]*errorHandlerWrapper)

func registerErrorHandler(fn ErrorHandler) int {
	errorHandlerMu.Lock()
	defer errorHandlerMu.Unlock()
	errorHandlerIndex++
	for errorHandlerIndex == 0 || errorHandlers[errorHandlerIndex] != nil {
		errorHandlerIndex++
	}
	errorHandlers[errorHandlerIndex] = &errorHandlerWrapper{fn: fn}
	return errorHandlerIndex
}

func getErrorHandler(i int) *errorHandlerWrapper {
	errorHandlerMu.Lock()
	defer errorHandlerMu.Unlock()
	return errorHandlers[i]
}

func unregisterErrorHandler(i int) {
	errorHandlerMu.Lock()
	defer errorHandlerMu.Unlock()
	delete(errorHandlers, i)
}

type errorAndLoggingOpts struct {
	eh     ErrorHandler
	config []string
}

type errorCallback struct {
	fn ErrorHandler
}

type errorAndLoggingOption interface {
	setErrorAndLoggingOpt(elo *errorAndLoggingOpts)
}

func ErrLogger(fn ErrorHandler) interface {
	errorAndLoggingOption
	BandCreateMaskOption
	BandIOOption
	//BoundsOption
	BufferOption
	BuildOverviewsOption
	BuildVRTOption
	ClearOverviewsOption
	CloseOption
	CreateLayerOption
	CreateSpatialRefOption
	DatasetCreateMaskOption
	DatasetCreateOption
	DatasetIOOption
	DatasetTranslateOption
	DatasetVectorTranslateOption
	DatasetWarpIntoOption
	DatasetWarpOption
	DeleteFeatureOption
	FeatureCountOption
	FillBandOption
	GeoJSONOption
	GeometryTransformOption
	GeometryReprojectOption
	GeometryWKBOption
	GeometryWKTOption
	GetGeoTransformOption
	HistogramOption
	MetadataOption
	NewFeatureOption
	NewGeometryOption
	OpenOption
	PolygonizeOption
	RasterizeGeometryOption
	RasterizeOption
	SetColorInterpOption
	SetColorTableOption
	SetGeometryOption
	SetNoDataOption
	SetGeoTransformOption
	SetProjectionOption
	SetSpatialRefOption
	SimplifyOption
	TransformOption
	UpdateFeatureOption
	VSIHandlerOption
	VSIOpenOption
	VSIUnlinkOption
	WKTExportOption
} {
	return errorCallback{fn}
}

func (ec errorCallback) setErrorAndLoggingOpt(elo *errorAndLoggingOpts) {
	elo.eh = ec.fn
}
func (ec errorCallback) setBandCreateMaskOpt(o *bandCreateMaskOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setBandIOOpt(o *bandIOOpts) {
	o.errorHandler = ec.fn
}

/*
func (ec errorCallback) setBoundsOpt(o *boundsOpts) {
	o.errorHandler = ec.fn
}
*/
func (ec errorCallback) setBufferOpt(o *bufferOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setBuildOverviewsOpt(o *buildOvrOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setBuildVRTOpt(o *buildVRTOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setClearOverviewsOpt(o *clearOvrOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setCloseOpt(o *closeOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setCreateLayerOpt(o *createLayerOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setCreateSpatialRefOpt(o *createSpatialRefOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setDatasetCreateMaskOpt(o *dsCreateMaskOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setDatasetCreateOpt(o *dsCreateOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setDatasetIOOpt(o *datasetIOOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setDatasetTranslateOpt(o *dsTranslateOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setDatasetVectorTranslateOpt(o *dsVectorTranslateOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setDatasetWarpIntoOpt(o *dsWarpIntoOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setDatasetWarpOpt(o *dsWarpOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setDeleteFeatureOpt(o *deleteFeatureOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setFeatureCountOpt(o *featureCountOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setFillBandOpt(o *fillBandOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setGeojsonOpt(o *geojsonOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setGeometryTransformOpt(o *geometryTransformOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setGeometryReprojectOpt(o *geometryReprojectOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setGeometryWKBOpt(o *geometryWKBOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setGeometryWKTOpt(o *geometryWKTOpts) {
	o.errorHandler = ec.fn
}

func (ec errorCallback) setGetGeoTransformOpt(o *getGeoTransformOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setHistogramOpt(o *histogramOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setMetadataOpt(o *metadataOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setNewFeatureOpt(o *newFeatureOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setNewGeometryOpt(o *newGeometryOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setOpenOpt(oo *openOpts) {
	oo.errorHandler = ec.fn
}
func (ec errorCallback) setPolygonizeOpt(o *polygonizeOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setRasterizeGeometryOpt(o *rasterizeGeometryOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setRasterizeOpt(o *rasterizeOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setSetColorInterpOpt(ndo *setColorInterpOpts) {
	ndo.errorHandler = ec.fn
}
func (ec errorCallback) setSetColorTableOpt(ndo *setColorTableOpts) {
	ndo.errorHandler = ec.fn
}
func (ec errorCallback) setSetGeometryOpt(o *setGeometryOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setSetGeoTransformOpt(o *setGeoTransformOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setSetProjectionOpt(o *setProjectionOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setSetNoDataOpt(ndo *setNodataOpts) {
	ndo.errorHandler = ec.fn
}
func (ec errorCallback) setSetSpatialRefOpt(ndo *setSpatialRefOpts) {
	ndo.errorHandler = ec.fn
}
func (ec errorCallback) setSimplifyOpt(o *simplifyOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setTransformOpt(o *trnOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setUpdateFeatureOpt(o *updateFeatureOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setVSIHandlerOpt(o *vsiHandlerOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setVSIOpenOpt(o *vsiOpenOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setVSIUnlinkOpt(o *vsiUnlinkOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setWKTExportOpt(o *srWKTOpts) {
	o.errorHandler = ec.fn
}
