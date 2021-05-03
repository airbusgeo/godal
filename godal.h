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
	char *godalSetMetadataItem(GDALMajorObjectH mo, char *ckey, char *cval, char *cdom);
	GDALDatasetH godalOpen(const char *name, unsigned int nOpenFlags, const char *const *papszAllowedDrivers,
						   const char *const *papszOpenOptions, const char *const *papszSiblingFiles,
						   char **error, char **config);

	GDALDatasetH godalCreate(GDALDriverH drv, const char *name, int width, int height, int nbands, GDALDataType dtype,
							 char **options, char **errmsg, char **config);

	void godalClose(GDALDatasetH ds, char **error);
	int godalRegisterDriver(const char *funcname);
	void godalRasterSize(GDALDatasetH ds, int *xsize, int *ysize);

	//returns a null terminated list of bands. the caller must free the returned list
	GDALRasterBandH *godalRasterBands(GDALDatasetH ds);
	OGRLayerH *godalVectorLayers(GDALDatasetH ds);
	
	GDALRasterBandH* godalBandOverviews(GDALRasterBandH bnd);

	char *godalSetRasterNoDataValue(GDALRasterBandH bnd, double nd);
	char *godalSetDatasetNoDataValue(GDALDatasetH bnd, double nd);
	char *godalDeleteRasterNoDataValue(GDALRasterBandH bnd);
	char *godalSetRasterColorInterpretation(GDALRasterBandH bnd, GDALColorInterp ci);
	GDALRasterBandH godalCreateMaskBand(GDALRasterBandH bnd, int flags, char **error, char **config);
	GDALRasterBandH godalCreateDatasetMaskBand(GDALDatasetH ds, int flags, char **error, char **config);
	OGRSpatialReferenceH godalCreateWKTSpatialRef(char *wkt, char **error);
	OGRSpatialReferenceH godalCreateProj4SpatialRef(char *proj, char **error);
	OGRSpatialReferenceH godalCreateEPSGSpatialRef(int epsgCode, char **error);
	char *godalExportToWKT(OGRSpatialReferenceH sr, char **error);
	OGRCoordinateTransformationH godalNewCoordinateTransformation( OGRSpatialReferenceH src, OGRSpatialReferenceH dst, char **error);
	char *godalDatasetSetSpatialRef(GDALDatasetH ds, OGRSpatialReferenceH sr);
	char *godalSetGeoTransform(GDALDatasetH ds, double *gt);
	char *godalGetGeoTransform(GDALDatasetH ds, double *gt);
	char *godalSetProjection(GDALDatasetH ds, char *wkt);

	GDALDatasetH godalTranslate(char *dstName, GDALDatasetH ds, char **switches, char **error, char **config);
	GDALDatasetH godalDatasetWarp(char *dstName, int nSrcCount, GDALDatasetH *srcDS, char **switches, char **error, char **config);
	char *godalDatasetWarpInto(GDALDatasetH dstDs,  int nSrcCount, GDALDatasetH *srcDS, char **switches, char **config);
	GDALDatasetH godalDatasetVectorTranslate(char *dstName, GDALDatasetH ds, char **switches, char **error, char **config);
	GDALDatasetH godalRasterize(char *dstName, GDALDatasetH ds, char **switches, char **error, char **config);
	char *godalRasterizeGeometry(GDALDatasetH ds, OGRGeometryH geom, int *bands, int nBands, double *vals, int allTouched);
	char *godalBuildOverviews(GDALDatasetH ds, const char *resampling, int nLevels, int *levels, int nBands, int *bands, char **config);
	char *godalClearOverviews(GDALDatasetH ds);

	void godalDatasetStructure(GDALDatasetH ds, int *sx, int *sy, int *bsx, int *bsy, int *bandCount, int *dtype);
	void godalBandStructure(GDALRasterBandH bnd, int *sx, int *sy, int *bsx, int *bsy, int *dtype);
	char *godalDatasetRasterIO(GDALDatasetH ds, GDALRWFlag rw, int nDSXOff, int nDSYOff, int nDSXSize, int nDSYSize, void *pBuffer,
		int nBXSize, int nBYSize, GDALDataType eBDataType, int nBandCount, int *panBandCount,
		int nPixelSpace, int nLineSpace, int nBandSpace, GDALRIOResampleAlg alg, char **config);
	char *godalBandRasterIO(GDALRasterBandH bnd, GDALRWFlag rw, int nDSXOff, int nDSYOff, int nDSXSize, int nDSYSize, void *pBuffer,
		int nBXSize, int nBYSize, GDALDataType eBDataType, int nPixelSpace, int nLineSpace, GDALRIOResampleAlg alg, char **config);
	char *godalFillRaster(GDALRasterBandH bnd, double real, double imag);
	char *godalPolygonize(GDALRasterBandH in, GDALRasterBandH mask, OGRLayerH layer, int fieldIndex, char **opts);

	char *godalLayerFeatureCount(OGRLayerH layer, int *count);
	char *godalLayerSetFeature(OGRLayerH layer, OGRFeatureH feat);
	OGRFeatureH godalLayerNewFeature(OGRLayerH layer, OGRGeometryH geom, char **error);
	char *godalLayerDeleteFeature(OGRLayerH layer, OGRFeatureH feat);
	char *godalFeatureSetGeometry(OGRFeatureH feat, OGRGeometryH geom);
	OGRLayerH godalCreateLayer(GDALDatasetH ds, char *name, OGRSpatialReferenceH sr, OGRwkbGeometryType gtype, char **error);
	char *VSIInstallGoHandler(const char *pszPrefix, size_t bufferSize, size_t cacheSize);

	void godalGetColorTable(GDALRasterBandH bnd, GDALPaletteInterp *interp, int *nEntries, short **entries);
	char* godalSetColorTable(GDALRasterBandH bnd, GDALPaletteInterp interp, int nEntries, short *entries);
	char *godalRasterHistogram(GDALRasterBandH bnd, double *min, double *max, int *buckets,
						   unsigned long long **values, int bIncludeOutOfRange, int bApproxOK);

	VSILFILE *godalVSIOpen(const char *name, char **error);
	char *godalVSIUnlink(const char *name);
	char* godalVSIClose(VSILFILE *f);
	size_t godalVSIRead(VSILFILE *f, void *buf, int len, char **error);
	OGRGeometryH godal_OGR_G_Simplify(OGRGeometryH in, double tolerance, char **errmsg);
	OGRGeometryH godal_OGR_G_Buffer(OGRGeometryH in, double tolerance, int segments, char **errmsg);
	OGRGeometryH godalNewGeometryFromWKT(char *wkt, OGRSpatialReferenceH sr, char **error);
	OGRGeometryH godalNewGeometryFromWKB(void *wkb, int wkbLen,OGRSpatialReferenceH sr, char **error);
	char* godalExportGeometryWKT(OGRGeometryH in, char **error);
	char* godalExportGeometryGeoJSON(OGRGeometryH in, int precision, char **error);
	char* godalExportGeometryWKB(void **wkb, int *wkbLen, OGRGeometryH in);
	char *godalGeometryTransformTo(OGRGeometryH geom, OGRSpatialReferenceH sr);
	char *godalGeometryTransform(OGRGeometryH geom, OGRCoordinateTransformationH trn, OGRSpatialReferenceH dst);

	GDALDatasetH godalBuildVRT(char *dstname, char **sources, char **switches, char **error, char **config);

	char *test_godal_error_handling(int debugEnabled, int loggerID, CPLErr failLevel);
#ifdef __cplusplus
}
#endif


#endif // GO_GDAL_H_
