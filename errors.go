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

import (
	"bytes"
	"errors"
	"sync"
)

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
	fn  ErrorHandler
	err error
}

var errorHandlers = make(map[int]*errorHandlerWrapper)

func registerErrorHandler(fn ErrorHandler) int {
	errorHandlerMu.Lock()
	defer errorHandlerMu.Unlock()
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

// ErrLogger is an option to override default error handling.
//
// See ErrorHandler.
func ErrLogger(fn ErrorHandler) interface {
	errorAndLoggingOption
	AddGeometryOption
	BandCreateMaskOption
	BandIOOption
	BoundsOption
	BufferOption
	BuildOverviewsOption
	BuildVRTOption
	ClearOverviewsOption
	CloseOption
	CopyLayerOption
	CreateFeatureOption
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
	DifferenceOption
	FeatureCountOption
	FillBandOption
	FillNoDataOption
	GeoJSONOption
	GeometryTransformOption
	GeometryReprojectOption
	GeometryWKBOption
	GeometryWKTOption
	GetGeoTransformOption
	GMLExportOption
	HistogramOption
	IntersectsOption
	IntersectionOption
	MetadataOption
	NewFeatureOption
	NewGeometryOption
	OpenOption
	PolygonizeOption
	RasterizeGeometryOption
	RasterizeOption
	RasterizeIntoOption
	SetColorInterpOption
	SetColorTableOption
	SetDescriptionOption
	SetGeometryOption
	SetFieldValueOption
	SetNoDataOption
	SetScaleOffsetOption
	SetGeoTransformOption
	SetGeometryColumnNameOption
	SetProjectionOption
	SetSpatialRefOption
	SieveFilterOption
	SimplifyOption
	SpatialRefValidateOption
	SubGeometryOption
	TransformOption
	UnionOption
	UpdateFeatureOption
	VSIHandlerOption
	VSIOpenOption
	VSIUnlinkOption
	WKTExportOption
	StatisticsOption
	SetStatisticsOption
	ClearStatisticsOption
	GridOption
	NearblackOption
	DemOption
	ViewshedOption
	SetGCPsOption
	GCPsToGeoTransformOption
	RegisterPluginOption
	ExecuteSQLOption
	StartTransactionOption
	CloseResultSetOption
	RollbackTransactionOption
	CommitTransactionOption
} {
	return errorCallback{fn}
}

func (ec errorCallback) setErrorAndLoggingOpt(elo *errorAndLoggingOpts) {
	elo.eh = ec.fn
}
func (ec errorCallback) setAddGeometryOpt(ao *addGeometryOpts) {
	ao.errorHandler = ec.fn
}
func (ec errorCallback) setBandCreateMaskOpt(o *bandCreateMaskOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setBandIOOpt(o *bandIOOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setBoundsOpt(o *boundsOpts) {
	o.errorHandler = ec.fn
}
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
func (ec errorCallback) setCopyLayerOpt(o *copyLayerOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setCreateFeatureOpt(cfo *createFeatureOpts) {
	cfo.errorHandler = ec.fn
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
func (ec errorCallback) setDifferenceOpt(do *differenceOpts) {
	do.errorHandler = ec.fn
}
func (ec errorCallback) setFeatureCountOpt(o *featureCountOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setFillBandOpt(o *fillBandOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setFillnodataOpt(o *fillnodataOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setGeojsonOpt(o *geojsonOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setGeometryColumnNameOpt(o *setGeometryColumnNameOpts) {
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
func (ec errorCallback) setGMLExportOpt(o *gmlExportOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setHistogramOpt(o *histogramOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setIntersectsOpt(o *intersectsOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setIntersectionOpt(o *intersectionOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setMetadataOpt(o *metadataOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setDescriptionOpt(o *setDescriptionOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setNewFeatureOpt(o *newFeatureOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setNewGeometryOpt(o *newGeometryOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setOpenOpt(oo *openOpts) error {
	oo.errorHandler = ec.fn
	return nil
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
func (ec errorCallback) setRasterizeIntoOpt(o *rasterizeIntoOpts) {
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
func (ec errorCallback) setSetFieldValueOpt(o *setFieldValueOpts) {
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
func (ec errorCallback) setSetScaleOffsetOpt(soo *setScaleOffsetOpts) {
	soo.errorHandler = ec.fn
}
func (ec errorCallback) setSetSpatialRefOpt(ndo *setSpatialRefOpts) {
	ndo.errorHandler = ec.fn
}
func (ec errorCallback) setSieveFilterOpt(sfo *sieveFilterOpts) {
	sfo.errorHandler = ec.fn
}
func (ec errorCallback) setSimplifyOpt(o *simplifyOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setSpatialRefValidateOpt(o *spatialRefValidateOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setSubGeometryOpt(so *subGeometryOpts) {
	so.errorHandler = ec.fn
}
func (ec errorCallback) setTransformOpt(o *trnOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setUnionOpt(uo *unionOpts) {
	uo.errorHandler = ec.fn
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

func (ec errorCallback) setStatisticsOpt(o *statisticsOpts) {
	o.errorHandler = ec.fn
}

func (ec errorCallback) setSetStatisticsOpt(o *setStatisticsOpt) {
	o.errorHandler = ec.fn
}

func (ec errorCallback) setClearStatisticsOpt(o *clearStatisticsOpt) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setGridCreateOpt(o *gridCreateOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setGridOpt(o *gridOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setNearblackOpt(o *nearBlackOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setDemOpt(o *demOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setViewshedOpt(o *viewshedOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setSetGCPsOpt(o *setGCPsOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setGCPsToGeoTransformOpts(o *gcpsToGeoTransformOpts) {
	o.errorHandler = ec.fn
}
func (ec errorCallback) setRegisterPluginOpt(o *registerPluginOpts) {
	o.errorHandler = ec.fn
}

func (ec errorCallback) setExecuteSQLOpt(o *executeSQLOpts) {
	o.errorHandler = ec.fn
}

func (ec errorCallback) setReleaseResultSetOpt(o *closeResultSetOpts) {
	o.errorHandler = ec.fn
}

func (ec errorCallback) setStartTransactionOpt(o *startTransactionOpts) {
	o.errorHandler = ec.fn
}

func (ec errorCallback) setRollbackTransactionOpt(o *rollbackTransactionOpts) {
	o.errorHandler = ec.fn
}

func (ec errorCallback) setCommitTransactionOpt(o *commitTransactionOpts) {
	o.errorHandler = ec.fn
}

type multiError struct {
	errs []error
}

// Error is the standard error interface
func (me *multiError) Error() string {
	w := bytes.NewBufferString(me.errs[0].Error())
	for i := 1; i < len(me.errs); i++ {
		w.WriteByte('\n')
		w.WriteString(me.errs[i].Error())
	}
	return w.String()
}

// As is the standard error wrapping interface
func (me *multiError) As(target interface{}) bool {
	for _, err := range me.errs {
		if errors.As(err, target) {
			return true
		}
	}
	return false
}

// Is is the standard error wrapping interface
func (me *multiError) Is(target error) bool {
	for _, err := range me.errs {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func combine(e1, e2 error) error {
	switch {
	case e1 == nil:
		return e2
	case e2 == nil:
		return e1
	}
	if me1, ok := e1.(*multiError); ok {
		if me2, ok := e2.(*multiError); ok {
			me1.errs = append(me1.errs, me2.errs...)
		} else {
			me1.errs = append(me1.errs, e2)
		}
		return me1
	} else if me2, ok := e2.(*multiError); ok {
		me := &multiError{}
		me.errs = make([]error, 1, len(me2.errs)+1)
		me.errs[0] = e1
		me.errs = append(me.errs, me2.errs...)
		return me
	} else {
		return &multiError{errs: []error{e1, e2}}
	}
}

var SkipWarnings = ErrLogger(
	func(ec ErrorCategory, code int, message string) error {
		if ec > CE_Warning {
			return errors.New(message)
		}
		return nil
	})
