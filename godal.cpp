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

#define _GNU_SOURCE 1
#include "godal.h"
#include <gdal.h>
#include <ogr_srs_api.h>
#include <cpl_conv.h>
#include "cpl_port.h"
#include "cpl_string.h"
#include "cpl_vsi.h"
#include "cpl_vsi_virtual.h"
#include <gdal_frmts.h>
#include <ogrsf_frmts.h>
#include <dlfcn.h>
#include <cassert>

#include <gdal_utils.h>
#include <gdal_alg.h>

extern "C" {
	extern long long int _gogdalSizeCallback(char* key, char** errorString);
	extern int _gogdalMultiReadCallback(char* key, int nRanges, void* pocbuffers, void* coffsets, void* clengths, char** errorString);
	extern size_t _gogdalReadCallback(char* key, void* buffer, size_t off, size_t clen, char** errorString);
	extern int goErrorHandler(int loggerID, CPLErr lvl, int code, const char *msg);
}

static void godalErrorHandler(CPLErr e, CPLErrorNum n, const char* msg) {
	cctx *ctx = (cctx*)CPLGetErrorHandlerUserData();
	assert(ctx!=nullptr);
	if (ctx->handlerIdx !=0) {
		int ret = goErrorHandler(ctx->handlerIdx, e, n, msg);
		if(ret!=0 && ctx->failed==0) {
			ctx->failed=1;
		}
	} else {
		//let's be strict and treat all warnings as errors
		if (e < CE_Warning)
		{
			fprintf(stderr, "GDAL: %s\n", msg);
			return;
		}
		if (ctx->errMessage == nullptr)
		{
			ctx->errMessage = (char *)malloc(strlen(msg) + 1);
			strcpy(ctx->errMessage, msg);
		}
		else
		{
			ctx->errMessage = (char *)realloc(ctx->errMessage, strlen(ctx->errMessage) + strlen(msg) + 3);
			strcat(ctx->errMessage, "\n");
			strcat(ctx->errMessage, msg);
		}
	}
}

static void godalWrap(cctx *ctx) {
	CPLPushErrorHandlerEx(&godalErrorHandler,ctx);
	if(ctx->configOptions!=nullptr) {
		char **options = ctx->configOptions;
		for(char* option=*options; option; option=*(++options)) {
			char *idx = strchr(option,'=');
			if(idx) {
				*idx='\0';
				CPLSetThreadLocalConfigOption(option,idx+1);
				*idx='=';
			}
		}
	}
}

static void godalUnwrap() {
	cctx *ctx = (cctx*)CPLGetErrorHandlerUserData();
	CPLPopErrorHandler();
	if(ctx->configOptions!=nullptr) {
		char **options = ctx->configOptions;
		for(char* option=*options; option; option=*(++options)) {
			char *idx = strchr(option,'=');
			if(idx) {
				*idx='\0';
				CPLSetThreadLocalConfigOption(option,nullptr);
				*idx='=';
			}
		}
	}
}

inline int failed(cctx *ctx) {
	if (ctx->errMessage!=nullptr || ctx->failed!=0) {
		return 1;
	}
	return 0;
}

inline void forceError(cctx *ctx) {
	if (ctx->errMessage == nullptr && ctx->failed==0) {
		CPLError(CE_Failure, CPLE_AppDefined, "unknown error");
	}
}
inline void forceCPLError(cctx *ctx, CPLErr err) {
	if (ctx->errMessage == nullptr && ctx->failed==0) {
		CPLError(CE_Failure, CPLE_AppDefined, "unknown cpl error %d", err);
	}
}
inline void forceOGRError(cctx *ctx, OGRErr err) {
	if (ctx->errMessage == nullptr && ctx->failed==0) {
		CPLError(CE_Failure, CPLE_AppDefined, "unknown ogr error %d", err);
	}
}

void godalSetMetadataItem(cctx *ctx, GDALMajorObjectH mo, char *ckey, char *cval, char *cdom) {
	godalWrap(ctx);
	CPLErr ret = GDALSetMetadataItem(mo,ckey,cval,cdom);
	if(ret!=0) {
		forceCPLError(ctx, ret);
	}
	godalUnwrap();
}
void godalClearMetadata(cctx *ctx, GDALMajorObjectH mo, char *cdom) {
	godalWrap(ctx);
	CPLErr ret = GDALSetMetadata(mo,nullptr,cdom);
	if(ret!=0) {
		forceCPLError(ctx, ret);
	}
	godalUnwrap();
}

void godalSetRasterColorInterpretation(cctx *ctx, GDALRasterBandH bnd, GDALColorInterp ci) {
	godalWrap(ctx);
	CPLErr ret = GDALSetRasterColorInterpretation(bnd,ci);
	if(ret!=0) {
		forceCPLError(ctx, ret);
	}
	godalUnwrap();
}

GDALDatasetH godalOpen(cctx *ctx, const char *name, unsigned int nOpenFlags, const char *const *papszAllowedDrivers,
					const char *const *papszOpenOptions, const char *const *papszSiblingFiles) {
	godalWrap(ctx);
	GDALDatasetH ret = GDALOpenEx(name,nOpenFlags,papszAllowedDrivers,papszOpenOptions,papszSiblingFiles);
	if (ret == nullptr) {
		forceError(ctx);
	}
	godalUnwrap();
	return ret;
}

void godalClose(cctx *ctx, GDALDatasetH ds) {
	godalWrap(ctx);
	GDALClose(ds);
	godalUnwrap();
}

typedef void (*fn_def)(void);

int _go_registerDriver(const char *driver, const char *prefix) {
	char *fnname = (char*)calloc(1,strlen(driver)+strlen(prefix)+1);
	sprintf(fnname,"%s%s",prefix,driver);
	void *fcn = dlsym(RTLD_DEFAULT,fnname);
	free(fnname);
	if (fcn != nullptr) {
		fn_def fnptr = (fn_def)fcn;
		fnptr();
	} else {
		return 1;
	}
	return 0;
}

int godalRegisterDriver(const char *fnname) {
	void *fcn = dlsym(RTLD_DEFAULT,fnname);
	if (fcn != nullptr) {
		fn_def fnptr = (fn_def)fcn;
		fnptr();
		return 0;
	}
	return -1;
}

GDALDatasetH godalCreate(cctx *ctx, GDALDriverH drv, const char* name, int width, int height, int nbands,
							GDALDataType dtype, char **options) {
	godalWrap(ctx);
	GDALDatasetH ret = GDALCreate(drv,name,width,height,nbands,dtype,options);
	if (ret==nullptr) {
		forceError(ctx);
	}
	godalUnwrap();
	return ret;
}

void godalDatasetSetSpatialRef(cctx *ctx, GDALDatasetH ds, OGRSpatialReferenceH sr) {
	godalWrap(ctx);
	CPLErr ret = GDALSetSpatialRef(ds,sr);
	if (ret!=0) {
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

char *exportToWKT(cctx *ctx, OGRSpatialReferenceH sr) {
	char *pszSRS = nullptr;
	OGRErr gret = OSRExportToWkt(sr,&pszSRS);
	if (gret!=OGRERR_NONE && !failed(ctx)) {
		forceOGRError(ctx, gret);
	}
	if(failed(ctx)) {
		CPLFree(pszSRS);
		pszSRS=nullptr;
	}

	/*TODO: handle wkt2 retry
	if (gret!=OGRERR_NONE || failed(ctx)) {
		CPLFree(pszSRS);
		pszSRS = nullptr;
		const char *const apszOptions[] = {"FORMAT=WKT2", nullptr};
		gret = oOutputSRS.exportToWkt(&pszSRS, apszOptions);
		if (gret!=OGRERR_NONE || failed(ctx)) {
			forceOGRError(ctx, gret);
			godalUnwrap();
			return;
		}
	}
	*/
	return pszSRS;
}

void godalSetProjection(cctx *ctx, GDALDatasetH ds, char *wkt) {
	godalWrap(ctx);
	if(wkt==nullptr||*wkt==0) {
		CPLErr ret = GDALSetSpatialRef(ds,nullptr);
		if (ret != 0)
		{
			forceCPLError(ctx, ret);
		}
		godalUnwrap();
		return;
	}
	OGRSpatialReferenceH sr = OSRNewSpatialReference(nullptr);
	OSRSetAxisMappingStrategy(sr, OAMS_TRADITIONAL_GIS_ORDER);

	OGRErr gret = OSRSetFromUserInput(sr,wkt);
	if (gret!=OGRERR_NONE || failed(ctx)) {
		forceOGRError(ctx,gret);
		godalUnwrap();
		OSRDestroySpatialReference(sr);
		return;
	}
	CPLErr ret = GDALSetSpatialRef(ds,sr);
	if (ret!=0) {
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
	OSRDestroySpatialReference(sr);
}

char *godalExportToWKT(cctx *ctx, OGRSpatialReferenceH sr) {
	godalWrap(ctx);
	char *pszSRS = exportToWKT(ctx, sr);
	godalUnwrap();
	return pszSRS;
}

OGRSpatialReferenceH godalCreateWKTSpatialRef(cctx *ctx, char *wkt){
	godalWrap(ctx);
	OGRSpatialReferenceH sr = OSRNewSpatialReference(nullptr);
	OSRSetAxisMappingStrategy(sr, OAMS_TRADITIONAL_GIS_ORDER);
	OGRErr gret = OSRImportFromWkt(sr, &wkt);
	if(gret!=0) {
		forceOGRError(ctx,gret);
	}
	godalUnwrap();
	if( failed(ctx) ) {
		OSRDestroySpatialReference(sr);
		return nullptr;
	}
	return sr;
}

OGRSpatialReferenceH godalCreateProj4SpatialRef(cctx *ctx, char *proj) {
	godalWrap(ctx);
	OGRSpatialReferenceH sr = OSRNewSpatialReference(nullptr);
	OSRSetAxisMappingStrategy(sr, OAMS_TRADITIONAL_GIS_ORDER);
	OGRErr gret = OSRImportFromProj4(sr, proj);
	if(gret!=0) {
		forceOGRError(ctx,gret);
	}
	godalUnwrap();
	if( failed(ctx) ) {
		OSRDestroySpatialReference(sr);
		return nullptr;
	}
	return sr;
}

OGRSpatialReferenceH godalCreateEPSGSpatialRef(cctx *ctx, int epsgCode) {
	godalWrap(ctx);
	OGRSpatialReferenceH sr = OSRNewSpatialReference(nullptr);
	OSRSetAxisMappingStrategy(sr, OAMS_TRADITIONAL_GIS_ORDER);
	OGRErr gret = OSRImportFromEPSG(sr, epsgCode);
	if(gret!=0) {
		forceOGRError(ctx,gret);
	}
	godalUnwrap();
	if( failed(ctx) ) {
		OSRDestroySpatialReference(sr);
		return nullptr;
	}
	return sr;
}

OGRSpatialReferenceH godalCreateUserSpatialRef(cctx *ctx, char *userInput) {
	godalWrap(ctx);
	OGRSpatialReferenceH sr = OSRNewSpatialReference(nullptr);
	OSRSetAxisMappingStrategy(sr, OAMS_TRADITIONAL_GIS_ORDER);
	OGRErr gret = OSRSetFromUserInput(sr, userInput);
	if(gret!=0) {
		forceOGRError(ctx,gret);
	}
	godalUnwrap();
	if( failed(ctx) ) {
		OSRDestroySpatialReference(sr);
		return nullptr;
	}
	return sr;
}

void godalValidateSpatialRef(cctx *ctx, OGRSpatialReferenceH sr) {
	godalWrap(ctx);
	OGRErr gret = OSRValidate(sr);
	if(gret!=0) {
		forceOGRError(ctx,gret);
	}
	godalUnwrap();
}

OGRCoordinateTransformationH godalNewCoordinateTransformation(cctx *ctx, OGRSpatialReferenceH src, OGRSpatialReferenceH dst) {
	godalWrap(ctx);
	OGRCoordinateTransformationH tr = OCTNewCoordinateTransformation(src,dst);
	if ( tr == nullptr ) {
		forceError(ctx);
	}
	godalUnwrap();
	return tr;
}

void godalSetGeoTransform(cctx *ctx, GDALDatasetH ds, double *gt){
	godalWrap(ctx);
	CPLErr ret = GDALSetGeoTransform(ds,gt);
	if( ret != 0 ) {
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

void godalGetGeoTransform(cctx *ctx, GDALDatasetH ds, double *gt){
	godalWrap(ctx);
	CPLErr ret = GDALGetGeoTransform(ds,gt);
	if( ret != 0 ) {
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

void godalRasterSize(GDALDatasetH ds, int *xsize, int *ysize) {
	*xsize = GDALGetRasterXSize(ds);
	*ysize = GDALGetRasterYSize(ds);
}

GDALRasterBandH* godalBandOverviews(GDALRasterBandH bnd) {
	int count = GDALGetOverviewCount(bnd);
	if(count == 0) {
		return nullptr;
	}
	GDALRasterBandH *ret = (GDALRasterBandH*)malloc((count+1)*sizeof(GDALRasterBandH));
	ret[count]=nullptr;
	for(int i=0; i<count; i++) {
		ret[i]=GDALGetOverview(bnd,i);
	}
	return ret;
}

GDALRasterBandH* godalRasterBands(GDALDatasetH ds) {
	int count = GDALGetRasterCount(ds);
	if(count == 0) {
		return nullptr;
	}
	GDALRasterBandH *ret = (GDALRasterBandH*)malloc((count+1)*sizeof(GDALRasterBandH));
	ret[count]=nullptr;
	for(int i=0; i<count; i++) {
		ret[i]=GDALGetRasterBand(ds,i+1);
	}
	return ret;
}
OGRLayerH* godalVectorLayers(GDALDatasetH ds) {
	int count = GDALDatasetGetLayerCount(ds);
	if(count == 0) {
		return nullptr;
	}
	OGRLayerH *ret = (OGRLayerH*)malloc((count+1)*sizeof(OGRLayerH));
	ret[count]=nullptr;
	for(int i=0; i<count; i++) {
		ret[i]=GDALDatasetGetLayer(ds,i);
	}
	return ret;
}

void godalSetDatasetNoDataValue(cctx *ctx, GDALDatasetH ds, double nd) {
	godalWrap(ctx);
	int count = GDALGetRasterCount(ds);
	if(count==0) {
		CPLError(CE_Failure, CPLE_AppDefined, "cannot set nodata value on dataset with no raster bands");
		godalUnwrap();
		return;
	}
	CPLErr ret = CE_None;
	for(int i=1; i<=count;i++) {
		CPLErr br = GDALSetRasterNoDataValue(GDALGetRasterBand(ds,i),nd);
		if(br!=0 && ret==0) {
			ret = br;
		}
	}
	if ( ret != 0 ) {
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

void godalSetRasterNoDataValue(cctx *ctx, GDALRasterBandH bnd, double nd) {
	godalWrap(ctx);
	CPLErr ret = GDALSetRasterNoDataValue(bnd,nd);
	if(ret!=0){
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

void godalDeleteRasterNoDataValue(cctx *ctx, GDALRasterBandH bnd) {
	godalWrap(ctx);
	CPLErr ret = GDALDeleteRasterNoDataValue(bnd);
	if(ret!=0){
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

GDALRasterBandH godalCreateMaskBand(cctx *ctx, GDALRasterBandH bnd, int flags) {
	godalWrap(ctx);
	CPLErr ret = GDALCreateMaskBand(bnd, flags);
	if(ret!=0) {
		forceCPLError(ctx,ret);
		godalUnwrap();
		return nullptr;
	}
	GDALRasterBandH mbnd = GDALGetMaskBand(bnd);
	if( mbnd == nullptr ) {
		forceError(ctx);
	}
	godalUnwrap();
	return mbnd;
}
GDALRasterBandH godalCreateDatasetMaskBand(cctx *ctx, GDALDatasetH ds, int flags) {
	godalWrap(ctx);
	if (GDALGetRasterCount(ds) == 0) {
		CPLError(CE_Failure, CPLE_AppDefined, "cannot create mask band on dataset with no bands");
		godalUnwrap();
		return nullptr;
	}
	CPLErr ret = GDALCreateDatasetMaskBand(ds, flags);
	if(ret!=0) {
		forceCPLError(ctx, ret);
		godalUnwrap();
		return nullptr;
	}
	GDALRasterBandH mbnd = GDALGetMaskBand(GDALGetRasterBand(ds,1));
	if( mbnd == nullptr ) {
		forceError(ctx);
	}
	godalUnwrap();
	return mbnd;
}

GDALDatasetH godalTranslate(cctx *ctx, char *dstName, GDALDatasetH ds, char **switches) {
	godalWrap(ctx);
	GDALTranslateOptions *translateopts = GDALTranslateOptionsNew(switches,nullptr);
	if(failed(ctx)) {
		GDALTranslateOptionsFree(translateopts);
		godalUnwrap();
		return nullptr;
	}
	int usageErr=0;
	GDALDatasetH ret = GDALTranslate(dstName, ds, translateopts, &usageErr);
	GDALTranslateOptionsFree(translateopts);
	if(ret==nullptr || usageErr!=0) {
		forceError(ctx);
	}
	godalUnwrap();
	return ret;
}

GDALDatasetH godalDatasetWarp(cctx *ctx, char *dstName, int nSrcCount, GDALDatasetH *srcDS, char **switches) {
	godalWrap(ctx);
	GDALWarpAppOptions *warpopts = GDALWarpAppOptionsNew(switches,nullptr);
	if(failed(ctx)) {
		GDALWarpAppOptionsFree(warpopts);
		godalUnwrap();
		return nullptr;
	}
	int usageErr=0;
	GDALDatasetH ret = GDALWarp(dstName, nullptr, nSrcCount, srcDS, warpopts, &usageErr);
	GDALWarpAppOptionsFree(warpopts);
	if(ret==nullptr || usageErr!=0) {
		forceError(ctx);
	}
	godalUnwrap();
	return ret;
}

void godalDatasetWarpInto(cctx *ctx, GDALDatasetH dstDs,  int nSrcCount, GDALDatasetH *srcDS, char **switches) {
	godalWrap(ctx);
	GDALWarpAppOptions *warpopts = GDALWarpAppOptionsNew(switches,nullptr);
	if(failed(ctx)) {
		GDALWarpAppOptionsFree(warpopts);
		godalUnwrap();
		return;
	}
	int usageErr=0;
	GDALDatasetH ret = GDALWarp(nullptr, dstDs, nSrcCount, srcDS, warpopts, &usageErr);
	GDALWarpAppOptionsFree(warpopts);
	if(ret==nullptr || usageErr!=0) {
		forceError(ctx);
	}
	godalUnwrap();
}

GDALDatasetH godalDatasetVectorTranslate(cctx *ctx, char *dstName, GDALDatasetH ds, char **switches) {
	godalWrap(ctx);
	GDALVectorTranslateOptions *opts = GDALVectorTranslateOptionsNew(switches,nullptr);
	if(failed(ctx)) {
		GDALVectorTranslateOptionsFree(opts);
		godalUnwrap();
		return nullptr;
	}
	int usageErr=0;
	GDALDatasetH ret = GDALVectorTranslate(dstName, nullptr, 1, &ds, opts, &usageErr);
	GDALVectorTranslateOptionsFree(opts);
	if(ret==nullptr || usageErr!=0) {
		forceError(ctx);
	}
	godalUnwrap();
	return ret;
}

void godalClearOverviews(cctx *ctx, GDALDatasetH ds) {
	godalWrap(ctx);
	CPLErr ret = GDALBuildOverviews(ds,"NEAREST",0,nullptr,0,nullptr,nullptr,nullptr);
	if(ret!=0){
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

void godalBuildOverviews(cctx *ctx, GDALDatasetH ds, const char *resampling, int nLevels, int *levels,
						  int nBands, int *bands) {
	godalWrap(ctx);
	CPLErr ret = GDALBuildOverviews(ds,resampling,nLevels,levels,nBands,bands,nullptr,nullptr);
	if(ret!=0){
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

void godalDatasetStructure(GDALDatasetH ds, int *sx, int *sy, int *bsx, int *bsy, int *bandCount, int *dtype) {
	*sx=GDALGetRasterXSize(ds);
	*sy=GDALGetRasterYSize(ds);
	*bandCount=GDALGetRasterCount(ds);
	*dtype=GDT_Unknown;
	*bsx=*bsy=0;
	if ( *bandCount > 0 ) {
		GDALRasterBandH band = GDALGetRasterBand(ds,1);
		*dtype = GDALGetRasterDataType(band);
		GDALGetBlockSize(band,bsx,bsy);
	}
}
void godalBandStructure(GDALRasterBandH bnd, int *sx, int *sy, int *bsx, int *bsy, int *dtype) {
	*sx=GDALGetRasterBandXSize(bnd);
	*sy=GDALGetRasterBandYSize(bnd);
	*dtype=GDT_Unknown;
	*bsx=*bsy=0;
	*dtype = GDALGetRasterDataType(bnd);
	GDALGetBlockSize(bnd, bsx, bsy);
}

void godalBandRasterIO(cctx *ctx, GDALRasterBandH bnd, GDALRWFlag rw, int nDSXOff, int nDSYOff, int nDSXSize, int nDSYSize, void *pBuffer,
		int nBXSize, int nBYSize, GDALDataType eBDataType, int nPixelSpace, int nLineSpace, GDALRIOResampleAlg alg) {
	godalWrap(ctx);
	GDALRasterIOExtraArg exargs;
	INIT_RASTERIO_EXTRA_ARG(exargs);
	if (alg != GRIORA_NearestNeighbour) {
		exargs.eResampleAlg = alg;
	}
	CPLErr ret = GDALRasterIOEx(bnd, rw, nDSXOff, nDSYOff, nDSXSize, nDSYSize, pBuffer, nBXSize, nBYSize,
									 eBDataType, nPixelSpace, nLineSpace, &exargs);
	if(ret!=0){
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

void godalDatasetRasterIO(cctx *ctx, GDALDatasetH ds, GDALRWFlag rw, int nDSXOff, int nDSYOff, int nDSXSize, int nDSYSize, void *pBuffer,
		int nBXSize, int nBYSize, GDALDataType eBDataType, int nBandCount, int *panBandCount,
		int nPixelSpace, int nLineSpace, int nBandSpace, GDALRIOResampleAlg alg) {
	godalWrap(ctx);
	GDALRasterIOExtraArg exargs;
	INIT_RASTERIO_EXTRA_ARG(exargs);
	if (alg != GRIORA_NearestNeighbour) {
		exargs.eResampleAlg = alg;
	}
	CPLErr ret = GDALDatasetRasterIOEx(ds, rw, nDSXOff, nDSYOff, nDSXSize, nDSYSize, pBuffer, nBXSize, nBYSize,
									 eBDataType, nBandCount, panBandCount, nPixelSpace, nLineSpace, nBandSpace, &exargs);
	if(ret!=0){
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

void godalFillRaster(cctx *ctx, GDALRasterBandH bnd, double real, double imag) {
	godalWrap(ctx);
	CPLErr ret = GDALFillRaster(bnd,real,imag);
	if(ret!=0){
		forceCPLError(ctx,ret);
	}
	godalUnwrap();

}

void godalPolygonize(cctx *ctx, GDALRasterBandH in, GDALRasterBandH mask, OGRLayerH layer,int fieldIndex, char **opts) {
	godalWrap(ctx);
	if (fieldIndex >= OGR_FD_GetFieldCount(OGR_L_GetLayerDefn(layer))) {
		CPLError(CE_Failure, CPLE_AppDefined, "invalid fieldIndex");
		godalUnwrap();
		return;
	}
	CPLErr ret = GDALPolygonize(in,mask,layer,fieldIndex,opts,nullptr,nullptr);
	if(ret!=0){
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

void godalSieveFilter(cctx *ctx, GDALRasterBandH bnd, GDALRasterBandH mask, GDALRasterBandH dst, int sizeThreshold, int connectedNess) {
	godalWrap(ctx);
	CPLErr ret = GDALSieveFilter(bnd,mask,dst,sizeThreshold,connectedNess,nullptr,nullptr,nullptr);
	if(ret!=0){
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

void godalFillNoData(cctx *ctx, GDALRasterBandH in, GDALRasterBandH mask, int maxDistance, int iterations, char **opts) {
	godalWrap(ctx);
	CPLErr ret = GDALFillNodata(in,mask,maxDistance,0,iterations,opts,nullptr,nullptr);
	if(ret!=0){
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

GDALDatasetH godalRasterize(cctx *ctx, char *dstName, GDALDatasetH ds, char **switches) {
	godalWrap(ctx);
	GDALRasterizeOptions *ropts = GDALRasterizeOptionsNew(switches,nullptr);
	if(failed(ctx)) {
		GDALRasterizeOptionsFree(ropts);
		godalUnwrap();
		return nullptr;
	}
	int usageErr=0;
	GDALDatasetH ret = GDALRasterize(dstName, nullptr, ds, ropts, &usageErr);
	GDALRasterizeOptionsFree(ropts);
	if(ret==nullptr || usageErr!=0) {
		forceError(ctx);
	}
	godalUnwrap();
	return ret;
}

void godalRasterizeGeometry(cctx *ctx, GDALDatasetH ds, OGRGeometryH geom, int *bands, int nBands, double *vals, int allTouched) {
	const char *opts[2] = { "ALL_TOUCHED=TRUE",nullptr };
	char **copts=(char**)opts;
	if (!allTouched) {
		copts=nullptr;
	}
	godalWrap(ctx);
	CPLErr ret = GDALRasterizeGeometries(ds,nBands,bands,1,&geom,nullptr,nullptr,vals,copts,nullptr,nullptr);
	if(ret!=0){
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

void godalLayerDeleteFeature(cctx *ctx, OGRLayerH layer, OGRFeatureH feat) {
	godalWrap(ctx);
	GIntBig fid = OGR_F_GetFID(feat);
	if (fid == OGRNullFID) {
		CPLError(CE_Failure, CPLE_AppDefined, "cannot delete feature with no FID");
		godalUnwrap();
		return;
	}
	OGRErr gret = OGR_L_DeleteFeature(layer,fid);
	if(gret!=0){
		forceOGRError(ctx,gret);
	}
	godalUnwrap();
}

void godalLayerSetFeature(cctx *ctx, OGRLayerH layer, OGRFeatureH feat) {
	godalWrap(ctx);
	OGRErr gret = OGR_L_SetFeature(layer,feat);
	if(gret!=0){
		forceOGRError(ctx,gret);
	}
	godalUnwrap();
}

void godalFeatureSetGeometry(cctx *ctx, OGRFeatureH feat, OGRGeometryH geom) {
	godalWrap(ctx);
	OGRErr gret = OGR_F_SetGeometry(feat,geom);
	if(gret!=0){
		forceOGRError(ctx,gret);
	}
	godalUnwrap();
}

OGRGeometryH godal_OGR_G_Simplify(cctx *ctx, OGRGeometryH in, double tolerance) {
	godalWrap(ctx);
	OGRGeometryH ret = OGR_G_Simplify(in,tolerance);
	if(ret==nullptr) {
		forceError(ctx);
	}
	godalUnwrap();
	return ret;
}

OGRGeometryH godal_OGR_G_Buffer(cctx *ctx, OGRGeometryH in, double tolerance, int segments) {
	godalWrap(ctx);
	OGRGeometryH ret = OGR_G_Buffer(in,tolerance,segments);
	if(ret==nullptr) {
		forceError(ctx);
	}
	godalUnwrap();
	return ret;
}

int godal_OGR_G_Intersects(cctx *ctx, OGRGeometryH geom1, OGRGeometryH geom2) {
	godalWrap(ctx);
	int ret = OGR_G_Intersects(geom1, geom2);
	godalUnwrap();
	return ret;
}

OGRLayerH godalCreateLayer(cctx *ctx, GDALDatasetH ds, char *name, OGRSpatialReferenceH sr, OGRwkbGeometryType gtype) {
	godalWrap(ctx);
	OGRLayerH ret = OGR_DS_CreateLayer(ds,name,sr,gtype,nullptr);
	if(ret==nullptr) {
		forceError(ctx);
	}
	godalUnwrap();
	return ret;
}

OGRLayerH godalCopyLayer(cctx *ctx, GDALDatasetH ds, OGRLayerH layer, char *name) {
	godalWrap(ctx);
	OGRLayerH ret = OGR_DS_CopyLayer(ds,layer, name,nullptr);
	if(ret==nullptr) {
		forceError(ctx);
	}
	godalUnwrap();
	return ret;
}

void godalLayerGetExtent(cctx *ctx, OGRLayerH layer, OGREnvelope *envelope) {
	godalWrap(ctx);
	OGRErr gret = OGR_L_GetExtent(layer, envelope, 1);
	if (gret != OGRERR_NONE) {
		forceOGRError(ctx,gret);
	} else if(envelope==nullptr) {
		forceError(ctx);
	}
	godalUnwrap();
}

void godalLayerFeatureCount(cctx *ctx, OGRLayerH layer, int *count) {
	godalWrap(ctx);
	GIntBig gcount = OGR_L_GetFeatureCount(layer, 1);
	*count=(int)gcount;
	godalUnwrap();
}

void godalGetColorTable(GDALRasterBandH bnd, GDALPaletteInterp *interp, int *nEntries, short **entries) {
	GDALColorTableH ct = GDALGetRasterColorTable(bnd);
	if( ct == nullptr ) {
		*interp=GPI_Gray;
		*nEntries=0;
		*entries=nullptr;
		return;
	}
	*interp = GDALGetPaletteInterpretation(ct);
	*nEntries = GDALGetColorEntryCount(ct);
	*entries = (short*)malloc(*nEntries*4*sizeof(short));
	for (int i=0;i<*nEntries;i++) {
		const GDALColorEntry *ce = GDALGetColorEntry(ct,i);
		(*entries)[i*4]=ce->c1;
		(*entries)[i*4+1]=ce->c2;
		(*entries)[i*4+2]=ce->c3;
		(*entries)[i*4+3]=ce->c4;
	}
}

void godalSetColorTable(cctx *ctx, GDALRasterBandH bnd, GDALPaletteInterp interp, int nEntries, short *entries) {
	godalWrap(ctx);
	CPLErr gret;
	if (nEntries == 0)
	{
		gret = GDALSetRasterColorTable(bnd, nullptr);
	}
	else
	{
		GDALColorTableH ct = GDALCreateColorTable(interp);
		for (int i = nEntries - 1; i >= 0; i--)
		{
			GDALColorEntry gce = {entries[i * 4], entries[i * 4 + 1], entries[i * 4 + 2], entries[i * 4 + 3]};
			GDALSetColorEntry(ct, i, &gce);
		}
		gret = GDALSetRasterColorTable(bnd, ct);
		GDALDestroyColorTable(ct);
	}
	if (gret != 0) {
		forceOGRError(ctx,gret);
	}
	godalUnwrap();
}

VSILFILE *godalVSIOpen(cctx *ctx, const char *name) {
	godalWrap(ctx);
	VSILFILE *fp = VSIFOpenExL(name,"r",1);
	if(fp==nullptr) {
		forceError(ctx);
	}
	if(failed(ctx)&&fp!=nullptr) {
		VSIFCloseL(fp);
		fp=nullptr;
	}
	godalUnwrap();
	return fp;
}

void godalVSIUnlink(cctx *ctx, const char *fname) {
	godalWrap(ctx);
	int ret = VSIUnlink(fname);
	if(ret!=0) {
		forceError(ctx);
	}
	godalUnwrap();
}

char* godalVSIClose(VSILFILE *f) {
	cctx ctx{nullptr,0,0,nullptr};
	godalWrap(&ctx);
	int ret = VSIFCloseL(f);
	if(ret!=0) {
		forceError(&ctx);
	}
	godalUnwrap();
	return ctx.errMessage;
}

size_t godalVSIRead(VSILFILE *f, void *buf, int len, char **errmsg) {
	cctx ctx{nullptr,0,0,nullptr};
	godalWrap(&ctx);
	size_t read = VSIFReadL(buf,1,len,f);
	godalUnwrap();
	*errmsg=ctx.errMessage;
	return read;
}

void godalRasterHistogram(cctx *ctx, GDALRasterBandH bnd, double *min, double *max, int *buckets,
						   unsigned long long **values, int bIncludeOutOfRange, int bApproxOK) {
	godalWrap(ctx);
	CPLErr ret = CE_None;
	if (*buckets == 0) {
		ret=GDALGetDefaultHistogramEx(bnd,min,max,buckets,values,1,nullptr,nullptr);
	} else {
		*values = (unsigned long long*) VSIMalloc(*buckets*sizeof(GUIntBig));
		ret=GDALGetRasterHistogramEx(bnd,*min,*max,*buckets,*values,bIncludeOutOfRange,bApproxOK,nullptr,nullptr);
	}
	if (ret != 0) {
		forceCPLError(ctx,ret);
	}
	godalUnwrap();
}

void godalComputeRasterStatistics(cctx *ctx, GDALRasterBandH bnd, int bApproxOK, double *pdfMin, double *pdfMax, double *pdfMean, double *pdfStdDev){
  godalWrap(ctx);
  CPLErr ret = CE_None;
  ret = GDALComputeRasterStatistics(bnd, bApproxOK, pdfMin, pdfMax, pdfMean, pdfStdDev, nullptr, nullptr);
  if (ret != 0) {
    forceCPLError(ctx,ret);
  }
  godalUnwrap();
}

int godalGetRasterStatistics(cctx *ctx, GDALRasterBandH bnd, int bApproxOK, double *pdfMin, double *pdfMax, double *pdfMean, double *pdfStdDev){
  godalWrap(ctx);
  CPLErr ret = CE_None;
  ret = GDALGetRasterStatistics(bnd, bApproxOK, 0, pdfMin, pdfMax, pdfMean, pdfStdDev);
  if (ret != 0 && ret != CE_Warning) {
    forceCPLError(ctx,ret);
  }
  godalUnwrap();
  return (ret == 0);
}


void godalSetRasterStatistics(cctx *ctx, GDALRasterBandH bnd, double dfMin, double dfMax, double dfMean, double dfStdDev){
  godalWrap(ctx);
  CPLErr ret = CE_None;
  ret = GDALSetRasterStatistics(bnd, dfMin, dfMax, dfMean, dfStdDev);
  if (ret != 0) {
    forceCPLError(ctx,ret);
  }
  godalUnwrap();
}

void godalClearRasterStatistics(cctx *ctx, GDALDatasetH ds){
  godalWrap(ctx);
#if GDAL_VERSION_NUM >= GDAL_COMPUTE_VERSION(3, 2, 0)
  GDALDatasetClearStatistics(ds);
#else
  CPLError(CE_Failure, CPLE_NotSupported, "GDALDatasetClearStatistics not supported with gdal < 3.2");
#endif
  godalUnwrap();
}

OGRGeometryH godalNewGeometryFromGeoJSON(cctx *ctx, char *geoJSON) {
	godalWrap(ctx);
	OGRGeometryH gptr = OGR_G_CreateGeometryFromJson(geoJSON);
	if (gptr == nullptr) {
		forceError(ctx);
	}
	if (failed(ctx) && gptr != nullptr) {
		OGR_G_DestroyGeometry(gptr);
		gptr = nullptr;
	}
	godalUnwrap();
	return gptr;
}

OGRGeometryH godalNewGeometryFromWKT(cctx *ctx, char *wkt, OGRSpatialReferenceH sr) {
	godalWrap(ctx);
	OGRGeometryH gptr = nullptr;
	char **wktPtr = &wkt;
	OGRErr gret = OGR_G_CreateFromWkt(wktPtr,sr,&gptr);
	if (gret != OGRERR_NONE) {
		forceOGRError(ctx,gret);
	} else if(gptr==nullptr) {
		forceError(ctx);
	}
	if(failed(ctx) && gptr!=nullptr) {
		OGR_G_DestroyGeometry(gptr);
		gptr=nullptr;
	}
	godalUnwrap();
	return gptr;
}

OGRGeometryH godalNewGeometryFromWKB(cctx *ctx, void *wkb, int wkbLen, OGRSpatialReferenceH sr) {
	godalWrap(ctx);
	OGRGeometryH gptr=nullptr;
	OGRErr gret = OGR_G_CreateFromWkb(wkb,sr,&gptr, wkbLen);
	if (gret != OGRERR_NONE) {
		forceOGRError(ctx,gret);
	} else if(gptr==nullptr) {
		forceError(ctx);
	}
	if(failed(ctx) && gptr!=nullptr) {
		OGR_G_DestroyGeometry(gptr);
		gptr=nullptr;
	}
	godalUnwrap();
	return gptr;
}

char* godalExportGeometryWKT(cctx *ctx, OGRGeometryH in) {
	godalWrap(ctx);
	char *wkt=nullptr;
	OGRErr gret = OGR_G_ExportToWkt(in,&wkt);
	if (gret != OGRERR_NONE) {
		forceOGRError(ctx,gret);
	} else if(wkt==nullptr) {
		forceError(ctx);
	}
	if(failed(ctx) && wkt!=nullptr) {
		CPLFree(wkt);
		wkt=nullptr;
	}
	godalUnwrap();
	return wkt;
}

void godalExportGeometryWKB(cctx *ctx, void **wkb, int *wkbLen, OGRGeometryH in) {
	godalWrap(ctx);
	*wkbLen=OGR_G_WkbSize(in);
	if (*wkbLen == 0) {
		*wkb=nullptr;
		godalUnwrap();
		return;
	}
	*wkb = malloc(*wkbLen);
	OGRErr gret = OGR_G_ExportToIsoWkb(in,wkbNDR,(unsigned char*)*wkb);
	if (gret != 0) {
		forceOGRError(ctx,gret);
		free(*wkb);
		*wkb=nullptr;
	}
	godalUnwrap();
}

char* godalExportGeometryGeoJSON(cctx *ctx, OGRGeometryH in, int precision) {
	godalWrap(ctx);
	char* opts[2];
	char precOpt[64];
	snprintf(precOpt,64,"COORDINATE_PRECISION=%d",precision);
	opts[0]=precOpt;
	opts[1]=nullptr;
	char *gj = OGR_G_ExportToJsonEx(in,opts);
	if (gj==nullptr) {
		forceError(ctx);
	}
	if (failed(ctx) && gj!=nullptr) {
		CPLFree(gj);
		gj=nullptr;
	}
	godalUnwrap();
	return gj;
}

void godalGeometryTransformTo(cctx *ctx, OGRGeometryH geom, OGRSpatialReferenceH sr) {
	godalWrap(ctx);
	OGRErr gret = OGR_G_TransformTo(geom,sr);
	if (gret != 0) {
		forceOGRError(ctx,gret);
	}
	OGR_G_AssignSpatialReference(geom, sr);
	godalUnwrap();
}

void godalGeometryTransform(cctx *ctx, OGRGeometryH geom, OGRCoordinateTransformationH trn, OGRSpatialReferenceH dst) {
	godalWrap(ctx);
	OGRErr gret = OGR_G_Transform(geom,trn);
	if (gret != 0) {
		forceOGRError(ctx,gret);
	}
	OGR_G_AssignSpatialReference(geom, dst);
	godalUnwrap();
}

void godalLayerCreateFeature(cctx *ctx, OGRLayerH layer, OGRFeatureH feat) {
	godalWrap(ctx);
	OGRErr oe = OGR_L_CreateFeature(layer,feat);
	if(oe != OGRERR_NONE) {
		forceOGRError(ctx,oe);
	}
	godalUnwrap();
}

OGRFeatureH godalLayerNewFeature(cctx *ctx, OGRLayerH layer, OGRGeometryH geom) {
	godalWrap(ctx);
	OGRFeatureH hFeature = OGR_F_Create( OGR_L_GetLayerDefn( layer ) );
	if(hFeature==nullptr) {
		forceError(ctx);
		godalUnwrap();
		return nullptr;
	}
	OGRErr oe=OGRERR_NONE;
	if (hFeature!=nullptr && geom!=nullptr) {
		oe = OGR_F_SetGeometry(hFeature,geom);
		if (oe == OGRERR_NONE) {
			oe = OGR_L_CreateFeature(layer,hFeature);
		}
	}
	if(oe != OGRERR_NONE) {
		forceOGRError(ctx,oe);
	}
	if(failed(ctx) && hFeature!=nullptr) {
		OGR_F_Destroy(hFeature);
		hFeature=nullptr;
	}
	godalUnwrap();
	return hFeature;
}

GDALDatasetH godalBuildVRT(cctx *ctx, char *dstName, char **sources, char **switches) {
	godalWrap(ctx);
	GDALBuildVRTOptions *ropts = GDALBuildVRTOptionsNew(switches,nullptr);
	if(failed(ctx)) {
		GDALBuildVRTOptionsFree(ropts);
		godalUnwrap();
		return nullptr;
	}
	int usageErr=0;
	int nSources = 0;
	char **src = sources;
	for( char **src = sources; *src; src++) {
		nSources++;
	}

	GDALDatasetH ret = GDALBuildVRT(dstName, nSources, nullptr, sources, ropts, &usageErr);
	GDALBuildVRTOptionsFree(ropts);
	if(ret==nullptr || usageErr!=0) {
		forceError(ctx);
	}
	godalUnwrap();
	return ret;
}

namespace cpl
{

    /************************************************************************/
    /*                     VSIGoFilesystemHandler                         */
    /************************************************************************/

    class VSIGoFilesystemHandler : public VSIFilesystemHandler
    {
        CPL_DISALLOW_COPY_ASSIGN(VSIGoFilesystemHandler)
    private:
        size_t m_buffer, m_cache;

    public:
        VSIGoFilesystemHandler(size_t bufferSize, size_t cacheSize);
        ~VSIGoFilesystemHandler() override;

		VSIVirtualHandle *Open(const char *pszFilename,
							   const char *pszAccess,
							   bool bSetError
#if GDAL_VERSION_NUM >= GDAL_COMPUTE_VERSION(3, 3, 0)
							   , CSLConstList /*papszOptions*/
#endif
							   ) override;

		int Stat(const char *pszFilename, VSIStatBufL *pStatBuf, int nFlags) override;
#if GDAL_VERSION_NUM >= 3020000
        char **SiblingFiles(const char *pszFilename) override;
#endif
        int HasOptimizedReadMultiRange(const char *pszPath) override;
    };

    /************************************************************************/
    /*                           VSIGoHandle                              */
    /************************************************************************/

    class VSIGoHandle : public VSIVirtualHandle
    {
        CPL_DISALLOW_COPY_ASSIGN(VSIGoHandle)
    private:
        char *m_filename;
        vsi_l_offset m_cur, m_size;
        int m_eof;

    public:
        VSIGoHandle(const char *filename, vsi_l_offset size);
        ~VSIGoHandle() override;

        vsi_l_offset Tell() override;
        int Seek(vsi_l_offset nOffset, int nWhence) override;
        size_t Read(void *pBuffer, size_t nSize, size_t nCount) override;
        int ReadMultiRange(int nRanges, void **ppData, const vsi_l_offset *panOffsets, const size_t *panSizes) override;
        VSIRangeStatus GetRangeStatus(vsi_l_offset nOffset, vsi_l_offset nLength) override;
        int Eof() override;
        int Close() override;
        size_t Write(const void *pBuffer, size_t nSize, size_t nCount) override;
        int Flush() override;
        int Truncate(vsi_l_offset nNewSize) override;
    };

    VSIGoHandle::VSIGoHandle(const char *filename, vsi_l_offset size)
    {
        m_filename = strdup(filename);
        m_cur = 0;
        m_eof = 0;
        m_size = size;
    }

    VSIGoHandle::~VSIGoHandle()
    {
        free(m_filename);
    }

    size_t VSIGoHandle::Write(const void *pBuffer, size_t nSize, size_t nCount)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "Write not implemented for go handlers");
        return -1;
    }
    int VSIGoHandle::Flush() 
    {
        CPLError(CE_Failure, CPLE_AppDefined, "Flush not implemented for go handlers");
        return -1;
    }
    int VSIGoHandle::Truncate(vsi_l_offset nNewSize) 
    {
        CPLError(CE_Failure, CPLE_AppDefined, "Truncate not implemented for go handlers");
        return -1;
    }
    int VSIGoHandle::Seek(vsi_l_offset nOffset, int nWhence)
    {
        if (nWhence == SEEK_SET)
        {
            m_cur = nOffset;
        }
        else if (nWhence == SEEK_CUR)
        {
            m_cur += nOffset;
        }
        else
        {
            m_cur = m_size;
        }
        m_eof = 0;
        return 0;
    }

    vsi_l_offset VSIGoHandle::Tell()
    {
        return m_cur;
    }

    int VSIGoHandle::Eof()
    {
        return m_eof;
    }

    int VSIGoHandle::Close()
    {
        return 0;
    }

    size_t VSIGoHandle::Read(void *pBuffer, size_t nSize, size_t nCount)
    {
        if (nSize * nCount == 0)
        {
            return 0;
        }
        char *err = nullptr;
        size_t read = _gogdalReadCallback(m_filename, pBuffer, m_cur, nSize * nCount, &err);
        if (err)
        {
            CPLError(CE_Failure, CPLE_AppDefined, "%s", err);
            errno = EIO;
            free(err);
            return 0;
        }
        if (read != nSize * nCount)
        {
            m_eof = 1;
        }
        size_t readblocks = read / nSize;
        m_cur += readblocks * nSize;
        return readblocks;
    }

    int VSIGoHandle::ReadMultiRange(int nRanges, void **ppData, const vsi_l_offset *panOffsets, const size_t *panSizes)
    {
        int iRange;
        int nMergedRanges = 1;
        for (iRange = 0; iRange < nRanges - 1; iRange++)
        {
            if (panOffsets[iRange] + panSizes[iRange] != panOffsets[iRange + 1])
            {
                nMergedRanges++;
            }
        }
        char *err = nullptr;
        if (nMergedRanges == nRanges)
        {
            int ret = _gogdalMultiReadCallback(m_filename, nRanges, (void *)ppData, (void *)panOffsets, (void *)panSizes, &err);
            if (err)
            {
                CPLError(CE_Failure, CPLE_AppDefined, "%s", err);
                errno = EIO;
                free(err);
                return -1;
            }
            return ret;
        }

        vsi_l_offset *mOffsets = new vsi_l_offset[nMergedRanges];
        size_t *mSizes = new size_t[nMergedRanges];
        char **mData = new char *[nMergedRanges];

        int curRange = 0;
        mSizes[curRange] = panSizes[0];
        mOffsets[curRange] = panOffsets[0];
        for (iRange = 0; iRange < nRanges - 1; iRange++)
        {
            if (panOffsets[iRange] + panSizes[iRange] == panOffsets[iRange + 1])
            {
                mSizes[curRange] += panSizes[iRange + 1];
            }
            else
            {
                mData[curRange] = new char[mSizes[curRange]];
                //start a new range
                curRange++;
                mSizes[curRange] = panSizes[iRange + 1];
                mOffsets[curRange] = panOffsets[iRange + 1];
            }
        }
        mData[curRange] = new char[mSizes[curRange]];

        int ret = _gogdalMultiReadCallback(m_filename, nRanges, (void *)ppData, (void *)panOffsets, (void *)panSizes, &err);

        if (err == nullptr)
        {
            curRange = 0;
            size_t curOffset = panSizes[0];
            memcpy(ppData[0], mData[0], panSizes[0]);
            for (iRange = 0; iRange < nRanges - 1; iRange++)
            {
                if (panOffsets[iRange] + panSizes[iRange] == panOffsets[iRange + 1])
                {
                    memcpy(ppData[iRange + 1], mData[curRange] + curOffset, panSizes[iRange + 1]);
                    curOffset += panSizes[iRange + 1];
                }
                else
                {
                    curRange++;
                    memcpy(ppData[iRange + 1], mData[curRange], panSizes[iRange + 1]);
                    curOffset = panSizes[iRange + 1];
                }
            }
        }
        else
        {
            CPLError(CE_Failure, CPLE_AppDefined, "%s", err);
            errno = EIO;
            free(err);
            ret = -1;
        }

        delete[] mOffsets;
        delete[] mSizes;
        for (int i = 0; i < nMergedRanges; i++)
        {
            delete[] mData[i];
        }
        delete[] mData;

        return ret;
    }

    VSIRangeStatus VSIGoHandle::GetRangeStatus(vsi_l_offset nOffset, vsi_l_offset nLength)
    {
        return VSI_RANGE_STATUS_UNKNOWN;
    }

    VSIGoFilesystemHandler::VSIGoFilesystemHandler(size_t bufferSize, size_t cacheSize)
    {
        m_buffer = bufferSize;
        m_cache = (cacheSize < bufferSize) ? bufferSize : cacheSize;
    }
    VSIGoFilesystemHandler::~VSIGoFilesystemHandler() {}

	VSIVirtualHandle *VSIGoFilesystemHandler::Open(const char *pszFilename,
												   const char *pszAccess,
												   bool bSetError
#if GDAL_VERSION_NUM >= GDAL_COMPUTE_VERSION(3, 3, 0)
												   , CSLConstList /*papszOptions*/
#endif
	)
	{
		if (strchr(pszAccess, 'w') != NULL ||
            strchr(pszAccess, '+') != NULL)
        {
            CPLError(CE_Failure, CPLE_AppDefined, "Only read-only mode is supported");
            return nullptr;
        }
        char *err = nullptr;
        long long s = _gogdalSizeCallback((char *)pszFilename, &err);

        if (s == -1)
        {
            if (err != nullptr && bSetError)
            {
                VSIError(VSIE_FileError, "%s", err);
            }
            errno = ENOENT;
            return nullptr;
        }
        if (m_buffer == 0)
        {
            return new VSIGoHandle(pszFilename, s);
        }
        else
        {
            return VSICreateCachedFile(new VSIGoHandle(pszFilename, s), m_buffer, m_cache);
        }
	}

	int VSIGoFilesystemHandler::Stat(const char *pszFilename,
                                     VSIStatBufL *pStatBuf,
                                     int nFlags)
    {
        char *err = nullptr;
        long long s = _gogdalSizeCallback((char *)pszFilename, &err);
        if (s == -1)
        {
            if (nFlags & VSI_STAT_SET_ERROR_FLAG)
            {
                CPLError(CE_Failure, CPLE_AppDefined, "%s", err);
                errno = ENOENT;
            }
            return -1;
        }
        memset(pStatBuf, 0, sizeof(VSIStatBufL));
        pStatBuf->st_mode = S_IFREG;

        if (nFlags & VSI_STAT_SIZE_FLAG)
        {
            pStatBuf->st_size = s;
        }
        return 0;
    }

    int VSIGoFilesystemHandler::HasOptimizedReadMultiRange(const char * /*pszPath*/)
    {
        return TRUE;
    }

#if GDAL_VERSION_NUM >= 3020000
    char **VSIGoFilesystemHandler::SiblingFiles(const char *pszFilename)
    {
        return (char **)calloc(1, sizeof(char *));
    }
#endif

} // namespace cpl

void VSIInstallGoHandler(cctx *ctx, const char *pszPrefix, size_t bufferSize, size_t cacheSize)
{
	godalWrap(ctx);
    CSLConstList papszPrefix = VSIFileManager::GetPrefixes();
    for( size_t i = 0; papszPrefix && papszPrefix[i]; ++i ) {
        if(strcmp(papszPrefix[i],pszPrefix)==0) {
            CPLError(CE_Failure, CPLE_AppDefined, "handler already registered on prefix");
			godalUnwrap();
			return;
        }
    }
    VSIFilesystemHandler *poHandler = new cpl::VSIGoFilesystemHandler(bufferSize, cacheSize);
    const std::string sPrefix(pszPrefix);
    VSIFileManager::InstallHandler(sPrefix, poHandler);
	godalUnwrap();
}


void test_godal_error_handling(cctx *ctx) {
	godalWrap(ctx);
	CPLDebug("godal","this is a debug message");
	CPLError(CE_Warning, CPLE_AppDefined, "this is a warning message");
	CPLError(CE_Failure, CPLE_AppDefined, "this is a failure message");
	godalUnwrap();
}

