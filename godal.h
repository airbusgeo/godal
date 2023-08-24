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

#ifndef _GODAL_H_
#define _GODAL_H_

#define _GNU_SOURCE 1
#include <gdal.h>
#include <gdal_alg.h>
#include <ogr_srs_api.h>
#include <cpl_conv.h>
#include "cpl_port.h"
#include <gdal_frmts.h>

#if GDAL_VERSION_NUM < 3000000
	#error "this code is only compatible with gdal version >= 3.0"
#endif

#ifdef __cplusplus
extern "C" {
#endif
	typedef struct {
		char *errMessage;
		int handlerIdx;
		int failed;
		char **configOptions;
	} cctx;
	void godalSetMetadataItem(cctx *ctx, GDALMajorObjectH mo, char *ckey, char *cval, char *cdom);
	void godalSetDescription(cctx *ctx, GDALMajorObjectH mo, char *desc);
	void godalClearMetadata(cctx *ctx, GDALMajorObjectH mo, char *cdom);
	GDALDatasetH godalOpen(cctx *ctx, const char *name, unsigned int nOpenFlags, const char *const *papszAllowedDrivers,
						   const char *const *papszOpenOptions, const char *const *papszSiblingFiles);

	GDALDatasetH godalCreate(cctx *ctx, GDALDriverH drv, const char *name, int width, int height, int nbands,
							GDALDataType dtype, char **creationOption);

	void godalClose(cctx *ctx, GDALDatasetH ds);
	int godalRegisterDriver(const char *funcname);
	void godalRasterSize(GDALDatasetH ds, int *xsize, int *ysize);

	//returns a null terminated list of bands. the caller must free the returned list
	GDALRasterBandH *godalRasterBands(GDALDatasetH ds);
	OGRLayerH *godalVectorLayers(GDALDatasetH ds);

	GDALRasterBandH* godalBandOverviews(GDALRasterBandH bnd);

	void godalSetRasterNoDataValue(cctx *ctx, GDALRasterBandH bnd, double nd);
	void godalSetDatasetNoDataValue(cctx *ctx, GDALDatasetH bnd, double nd);
	void godalDeleteRasterNoDataValue(cctx *ctx, GDALRasterBandH bnd);
	void godalSetRasterScaleOffset(cctx *ctx, GDALRasterBandH bnd, double scale, double offset);
	void godalSetDatasetScaleOffset(cctx *ctx, GDALDatasetH bnd, double scale, double offset);
	void godalSetRasterColorInterpretation(cctx *ctx, GDALRasterBandH bnd, GDALColorInterp ci);
	GDALRasterBandH godalCreateMaskBand(cctx *ctx, GDALRasterBandH bnd, int flags);
	GDALRasterBandH godalCreateDatasetMaskBand(cctx *ctx, GDALDatasetH ds, int flags);
	OGRSpatialReferenceH godalCreateUserSpatialRef(cctx *ctx, char *userInput);
	OGRSpatialReferenceH godalCreateWKTSpatialRef(cctx *ctx, char *wkt);
	OGRSpatialReferenceH godalCreateProj4SpatialRef(cctx *ctx, char *proj);
	OGRSpatialReferenceH godalCreateEPSGSpatialRef(cctx *ctx, int epsgCode);
	void godalValidateSpatialRef(cctx *ctx, OGRSpatialReferenceH sr);
	char* godalExportToWKT(cctx *ctx, OGRSpatialReferenceH sr);
	OGRCoordinateTransformationH godalNewCoordinateTransformation(cctx *ctx,  OGRSpatialReferenceH src, OGRSpatialReferenceH dst);
	void godalDatasetSetSpatialRef(cctx *ctx, GDALDatasetH ds, OGRSpatialReferenceH sr);
	void godalSetGeoTransform(cctx *ctx, GDALDatasetH ds, double *gt);
	void godalGetGeoTransform(cctx *ctx, GDALDatasetH ds, double *gt);
	void godalSetProjection(cctx *ctx, GDALDatasetH ds, char *wkt);

	GDALDatasetH godalTranslate(cctx *ctx, char *dstName, GDALDatasetH ds, char **switches);
	GDALDatasetH godalDatasetWarp(cctx *ctx, char *dstName, int nSrcCount, GDALDatasetH *srcDS, char **switches);
	void godalDatasetWarpInto(cctx *ctx, GDALDatasetH dstDs,  int nSrcCount, GDALDatasetH *srcDS, char **switches);
	GDALDatasetH godalDatasetVectorTranslate(cctx *ctx, char *dstName, GDALDatasetH ds, char **switches);
	GDALDatasetH godalRasterize(cctx *ctx, char *dstName, GDALDatasetH dstDS, GDALDatasetH ds, char **switches);
	void godalRasterizeGeometry(cctx *ctx, GDALDatasetH ds, OGRGeometryH geom, int *bands, int nBands, double *vals, int allTouched);
	void godalBuildOverviews(cctx *ctx, GDALDatasetH ds, const char *resampling, int nLevels, int *levels, int nBands, int *bands);
	void godalClearOverviews(cctx *ctx, GDALDatasetH ds);

	void godalDatasetStructure(GDALDatasetH ds, int *sx, int *sy, int *bsx, int *bsy, double *scale, double *offset, int *bandCount, int *dtype);
	void godalBandStructure(GDALRasterBandH bnd, int *sx, int *sy, int *bsx, int *bsy, double *scale, double *offset, int *dtype);
	void godalDatasetRasterIO(cctx *ctx, GDALDatasetH ds, GDALRWFlag rw, int nDSXOff, int nDSYOff, int nDSXSize, int nDSYSize, void *pBuffer,
		int nBXSize, int nBYSize, GDALDataType eBDataType, int nBandCount, int *panBandCount,
		int nPixelSpace, int nLineSpace, int nBandSpace, GDALRIOResampleAlg alg);
	void godalBandRasterIO(cctx *ctx, GDALRasterBandH bnd, GDALRWFlag rw, int nDSXOff, int nDSYOff, int nDSXSize, int nDSYSize, void *pBuffer,
		int nBXSize, int nBYSize, GDALDataType eBDataType, int nPixelSpace, int nLineSpace, GDALRIOResampleAlg alg);
	void godalFillRaster(cctx *ctx, GDALRasterBandH bnd, double real, double imag);
	void godalPolygonize(cctx *ctx, GDALRasterBandH in, GDALRasterBandH mask, OGRLayerH layer, int fieldIndex, char **opts);
	void godalFillNoData(cctx *ctx, GDALRasterBandH in, GDALRasterBandH mask, int maxDistance, int iterations, char **opts);
	void godalSieveFilter(cctx *ctx, GDALRasterBandH bnd, GDALRasterBandH mask, GDALRasterBandH dst, int sizeThreshold, int connectedNess);

	void godalLayerGetExtent(cctx *ctx, OGRLayerH layer, OGREnvelope *envelope);
	void godalLayerFeatureCount(cctx *ctx, OGRLayerH layer, int *count);
	void godalLayerSetFeature(cctx *ctx, OGRLayerH layer, OGRFeatureH feat);
	void godalLayerCreateFeature(cctx *ctx, OGRLayerH layer, OGRFeatureH feat);
	OGRFeatureH godalLayerNewFeature(cctx *ctx, OGRLayerH layer, OGRGeometryH geom);
	void godalLayerDeleteFeature(cctx *ctx, OGRLayerH layer, OGRFeatureH feat);
	void godalFeatureSetGeometry(cctx *ctx, OGRFeatureH feat, OGRGeometryH geom);
	void godalFeatureSetFieldInteger(cctx *ctx, OGRFeatureH feat, int fieldIndex, int value);
	void godalFeatureSetFieldInteger64(cctx *ctx, OGRFeatureH feat, int fieldIndex, long long value);
	void godalFeatureSetFieldDouble(cctx *ctx, OGRFeatureH feat, int fieldIndex, double value);
	void godalFeatureSetFieldString(cctx *ctx, OGRFeatureH feat, int fieldIndex, char *value);
	void godalFeatureSetFieldDateTime(cctx *ctx, OGRFeatureH feat, int fieldIndex, int year, int month, int day, int hour, int minute, int second, int tzFlag);
	void godalFeatureSetFieldIntegerList(cctx *ctx, OGRFeatureH feat, int fieldIndex, int nbValues, int *values);
	void godalFeatureSetFieldInteger64List(cctx *ctx, OGRFeatureH feat, int fieldIndex, int nbValues, long long *values);
	void godalFeatureSetFieldDoubleList(cctx *ctx, OGRFeatureH feat, int fieldIndex, int nbValues, double *values);
	void godalFeatureSetFieldStringList(cctx *ctx, OGRFeatureH feat, int fieldIndex, char **values);
	void godalFeatureSetFieldBinary(cctx *ctx, OGRFeatureH feat, int fieldIndex, int nbBytes, void *value);
	OGRLayerH godalCreateLayer(cctx *ctx, GDALDatasetH ds, char *name, OGRSpatialReferenceH sr, OGRwkbGeometryType gtype);
	OGRLayerH godalCopyLayer(cctx *ctx, GDALDatasetH ds, OGRLayerH layer, char *name);
	void VSIInstallGoHandler(cctx *ctx, const char *pszPrefix, size_t bufferSize, size_t cacheSize);

	void godalGetColorTable(GDALRasterBandH bnd, GDALPaletteInterp *interp, int *nEntries, short **entries);
	void godalSetColorTable(cctx *ctx, GDALRasterBandH bnd, GDALPaletteInterp interp, int nEntries, short *entries);
	void godalRasterHistogram(cctx *ctx, GDALRasterBandH bnd, double *min, double *max, int *buckets,
						   unsigned long long **values, int bIncludeOutOfRange, int bApproxOK);

	VSILFILE *godalVSIOpen(cctx *ctx, const char *name);
	void godalVSIUnlink(cctx *ctx, const char *name);
	char* godalVSIClose(VSILFILE *f);
	size_t godalVSIRead(VSILFILE *f, void *buf, int len, char **errmsg);
	void godal_OGR_G_AddGeometry(cctx *ctx, OGRGeometryH geom, OGRGeometryH subGeom);
	OGRGeometryH godal_OGR_G_Simplify(cctx *ctx, OGRGeometryH in, double tolerance);
	OGRGeometryH godal_OGR_G_Buffer(cctx *ctx, OGRGeometryH in, double tolerance, int segments);
	OGRGeometryH godal_OGR_G_Difference(cctx *ctx, OGRGeometryH geom1, OGRGeometryH geom2);
	OGRGeometryH godal_OGR_G_GetGeometryRef(cctx *ctx, OGRGeometryH in, int subGeomIndex);
	int godal_OGR_G_Intersects(cctx *ctx, OGRGeometryH geom1, OGRGeometryH geom2);
	OGRGeometryH godal_OGR_G_Intersection(cctx *ctx, OGRGeometryH geom1, OGRGeometryH geom2);
	OGRGeometryH godal_OGR_G_Union(cctx *ctx, OGRGeometryH geom1, OGRGeometryH geom2);
	OGRGeometryH godalNewGeometryFromGeoJSON(cctx *ctx, char *geoJSON);
	OGRGeometryH godalNewGeometryFromWKT(cctx *ctx, char *wkt, OGRSpatialReferenceH sr);
	OGRGeometryH godalNewGeometryFromWKB(cctx *ctx, void *wkb, int wkbLen,OGRSpatialReferenceH sr);
	char* godalExportGeometryWKT(cctx *ctx, OGRGeometryH in);
	char* godalExportGeometryGeoJSON(cctx *ctx, OGRGeometryH in, int precision);
	char* godalExportGeometryGML(cctx *ctx, OGRGeometryH in, char **switches);
	void godalExportGeometryWKB(cctx *ctx, void **wkb, int *wkbLen, OGRGeometryH in);
	void godalGeometryTransformTo(cctx *ctx, OGRGeometryH geom, OGRSpatialReferenceH sr);
	void godalGeometryTransform(cctx *ctx, OGRGeometryH geom, OGRCoordinateTransformationH trn, OGRSpatialReferenceH dst);

	GDALDatasetH godalBuildVRT(cctx *ctx, char *dstname, char **sources, char **switches);

	void test_godal_error_handling(cctx *ctx);
    void godalClearRasterStatistics(cctx *ctx, GDALDatasetH ds);
    void godalComputeRasterStatistics(cctx *ctx, GDALRasterBandH bnd, int bApproxOK, double *pdfMin, double *pdfMax, double *pdfMean, double *pdfStdDev);
    int godalGetRasterStatistics(cctx *ctx, GDALRasterBandH bnd, int bApproxOK, double *pdfMin, double *pdfMax, double *pdfMean, double *pdfStdDev);
    void godalSetRasterStatistics(cctx *ctx, GDALRasterBandH bnd, double dfMin, double dfMax, double dfMean, double dfStdDev);
	void godalGridCreate(cctx *ctx, GDALGridAlgorithm eAlgorithm, void *ppOptions, GUInt32 nPoints, const double *padfX, const double *padfY, const double *padfZ, double dfXMin, double dfXMax, double dfYMin, double dfYMax, GUInt32 nXSize, GUInt32 nYSize, GDALDataType eType, void *pData);
	void godalGridParseAlgorithmAndOptions(cctx *ctx, char *pszAlgorithm, GDALGridAlgorithm *peAlgorithm, void **ppOptions);
#ifdef __cplusplus
}
#endif


#endif // GO_GDAL_H_
