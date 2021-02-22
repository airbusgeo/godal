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
#include "_cgo_export.h"

char *cplErrToString(CPLErr err) {
	const char *msg = "cpl error %d";
	char *str = (char *)malloc(strlen(msg) + 10);
	snprintf(str, strlen(msg) + 10, msg, err);
	return str;
}
char *ogrErrToString(OGRErr err) {
	const char *msg = "ogr error %d";
	char *str = (char *)malloc(strlen(msg) + 10);
	snprintf(str, strlen(msg) + 10, msg, err);
	return str;
}

static void godalUnwrap(char **options) {
	CPLPopErrorHandler();
	if(options!=nullptr) {
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

static void godalErrorHandler(CPLErr e, CPLErrorNum n, const char* msg) {
	//let's be strict and treat all warnings as errors
	if (e < CE_Warning) {
		fprintf(stderr,"GDAL INFO: %s\n",msg);
		return;
	}
	char **hmsg = (char**)CPLGetErrorHandlerUserData();
	assert(hmsg!=nullptr);
	if(*hmsg==nullptr) {
		*hmsg = (char*)malloc(strlen(msg)+1);
		strcpy(*hmsg,msg);
	} else {
		*hmsg = (char*)realloc(*hmsg,strlen(*hmsg)+strlen(msg)+3);
		strcat(*hmsg,"\n");
		strcat(*hmsg,msg);
	}
}

static void godalWrap(char **hmsg, char **options) {
	*hmsg=nullptr;
	CPLPushErrorHandlerEx(&godalErrorHandler,hmsg);
	if(options!=nullptr) {
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

char *godalSetMetadataItem(GDALMajorObjectH mo, char *ckey, char *cval, char *cdom) {
	char *error=nullptr;
	godalWrap(&error,nullptr);
	CPLErr ret = GDALSetMetadataItem(mo,ckey,cval,cdom);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if(ret!=0) {
		return cplErrToString(ret);
	}
	return nullptr;
}

char *godalSetRasterColorInterpretation(GDALRasterBandH bnd, GDALColorInterp ci) {
	char *error=nullptr;
	godalWrap(&error,nullptr);
	CPLErr ret = GDALSetRasterColorInterpretation(bnd,ci);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if(ret!=0) {
		return cplErrToString(ret);
	}
	return nullptr;
}

GDALDatasetH godalOpen(const char *name, unsigned int nOpenFlags, const char *const *papszAllowedDrivers,
					const char *const *papszOpenOptions, const char *const *papszSiblingFiles,
					char **error, char **config) {
	godalWrap(error, config);
	GDALDatasetH ret = GDALOpenEx(name,nOpenFlags,papszAllowedDrivers,papszOpenOptions,papszSiblingFiles);
	godalUnwrap(config);
	if (ret==nullptr && *error==nullptr) {
		*error=strdup("failed to open: unknown error");
	}
	return ret;
}

void godalClose(GDALDatasetH ds, char **error) {
	godalWrap(error,nullptr);
	GDALClose(ds);
	godalUnwrap(nullptr);
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

GDALDatasetH godalCreate(GDALDriverH drv, const char* name, int width, int height, int nbands,
							GDALDataType dtype, char **options, char **error, char **config) {
	godalWrap(error, config);
	GDALDatasetH ret = GDALCreate(drv,name,width,height,nbands,dtype,options);
	godalUnwrap(config);
	if (ret==nullptr && *error==nullptr) {
		*error=strdup("failed to create: unknown error");
	}
	return ret;
}

char *godalDatasetSetSpatialRef(GDALDatasetH ds, OGRSpatialReferenceH sr) {
	char *error=nullptr;
	godalWrap(&error,nullptr);
	CPLErr ret = GDALSetSpatialRef(ds,sr);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if(ret!=0) {
		return cplErrToString(ret);
	}
	return nullptr;
}

char *godalSetProjection(GDALDatasetH ds, char *wkt) {
	char *error=nullptr;
	godalWrap(&error,nullptr);
	CPLErr ret = GDALSetProjection(ds,wkt);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if(ret!=0) {
		return cplErrToString(ret);
	}
	return nullptr;
}

char *godalExportToWKT(OGRSpatialReferenceH sr, char **error) {
	godalWrap(error,nullptr);
	char *wkt = nullptr;
	OGRErr gret = OSRExportToWkt(sr, &wkt);
	godalUnwrap(nullptr);
	if (*error!=nullptr) {
		return nullptr;
	}
	if (gret != 0) {
		*error=ogrErrToString(gret);
		return nullptr;
	}
	return wkt;
}

OGRSpatialReferenceH godalCreateWKTSpatialRef(char *wkt, char **error){
	godalWrap(error,nullptr);
	OGRSpatialReferenceH sr = OSRNewSpatialReference(nullptr);
	OSRSetAxisMappingStrategy(sr, OAMS_TRADITIONAL_GIS_ORDER);
	OGRErr gret = OSRImportFromWkt(sr, &wkt);
	godalUnwrap(nullptr);
	if (*error!=nullptr) {
		return nullptr;
	}
	if (gret != 0) {
		*error=ogrErrToString(gret);
		return nullptr;
	}
	return sr;
}
OGRSpatialReferenceH godalCreateProj4SpatialRef(char *proj, char **error) {
	godalWrap(error,nullptr);
	OGRSpatialReferenceH sr = OSRNewSpatialReference(nullptr);
	OSRSetAxisMappingStrategy(sr, OAMS_TRADITIONAL_GIS_ORDER);
	OGRErr gret = OSRImportFromProj4(sr, proj);
	godalUnwrap(nullptr);
	if (*error!=nullptr) {
		return nullptr;
	}
	if (gret != 0) {
		*error=ogrErrToString(gret);
		return nullptr;
	}
	return sr;
}

OGRSpatialReferenceH godalCreateEPSGSpatialRef(int epsgCode, char **error) {
	godalWrap(error,nullptr);
	OGRSpatialReferenceH sr = OSRNewSpatialReference(nullptr);
	OSRSetAxisMappingStrategy(sr, OAMS_TRADITIONAL_GIS_ORDER);
	OGRErr gret = OSRImportFromEPSG(sr, epsgCode);
	godalUnwrap(nullptr);
	if (*error!=nullptr) {
		return nullptr;
	}
	if (gret != 0) {
		*error=ogrErrToString(gret);
		return nullptr;
	}
	return sr;
}

OGRCoordinateTransformationH godalNewCoordinateTransformation( OGRSpatialReferenceH src, OGRSpatialReferenceH dst, char **error) {
	godalWrap(error,nullptr);
	OGRCoordinateTransformationH tr = OCTNewCoordinateTransformation(src,dst);
	godalUnwrap(nullptr);
	if (*error!=nullptr) {
		return nullptr;
	}
	if (tr == nullptr) {
		*error=strdup("unknown error");
		return nullptr;
	}
	return tr;
}

char *godalSetGeoTransform(GDALDatasetH ds, double *gt){
	char *error=nullptr;
	godalWrap(&error,nullptr);
	CPLErr ret = GDALSetGeoTransform(ds,gt);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if(ret!=0) {
		return cplErrToString(ret);
	}
	return nullptr;
}
char *godalGetGeoTransform(GDALDatasetH ds, double *gt){
	char *error=nullptr;
	godalWrap(&error,nullptr);
	CPLErr ret = GDALGetGeoTransform(ds,gt);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if(ret!=0) {
		return cplErrToString(ret);
	}
	return nullptr;
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

char* godalSetDatasetNoDataValue(GDALDatasetH ds, double nd) {
	//First set nodata on all bands without checking for errors
	char *error=nullptr;
	int count = GDALGetRasterCount(ds);
	if(count==0) {
		return strdup("cannot set nodata on dataset with no bands");
	}
	godalWrap(&error,nullptr);
	for(int i=1; i<=count;i++) {
		GDALSetRasterNoDataValue(GDALGetRasterBand(ds,i),nd);
	}
	godalUnwrap(nullptr);
	free(error);

	//second pass where we actually check for errors
	error=nullptr;
	godalWrap(&error,nullptr);
	CPLErr ret=CPLErr(0);
	for(int i=1; i<=count;i++) {
		CPLErr rr = GDALSetRasterNoDataValue(GDALGetRasterBand(ds,i),nd);
		if (ret == 0) {
			ret =rr;
		}
	}
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if(ret!=0) {
		return cplErrToString(ret);
	}
	return nullptr;
}
char* godalSetRasterNoDataValue(GDALRasterBandH bnd, double nd) {
	char *error=nullptr;
	godalWrap(&error,nullptr);
	CPLErr ret = GDALSetRasterNoDataValue(bnd,nd);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if(ret!=0) {
		return cplErrToString(ret);
	}
	return nullptr;
}

char* godalDeleteRasterNoDataValue(GDALRasterBandH bnd) {
	char *error=nullptr;
	godalWrap(&error,nullptr);
	CPLErr ret = GDALDeleteRasterNoDataValue(bnd);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if(ret!=0) {
		return cplErrToString(ret);
	}
	return nullptr;
}

GDALRasterBandH godalCreateMaskBand(GDALRasterBandH bnd, int flags, char **error, char **config) {
	godalWrap(error,config);
	CPLErr ret = GDALCreateMaskBand(bnd, flags);
	godalUnwrap(config);
	if(*error!=nullptr) {
		return nullptr;
	}
	if(ret!=0) {
		*error = cplErrToString(ret);
		return nullptr;
	}
	GDALRasterBandH mbnd = GDALGetMaskBand(bnd);
	if( mbnd == nullptr ) {
		*error = strdup("unknown error");
	}
	return mbnd;
}
GDALRasterBandH godalCreateDatasetMaskBand(GDALDatasetH ds, int flags, char **error, char **config) {
	if (GDALGetRasterCount(ds) == 0) {
		*error = strdup("cannot create mask band on dataset with no bands");
		return nullptr;
	}
	godalWrap(error,config);
	CPLErr ret = GDALCreateDatasetMaskBand(ds, flags);
	godalUnwrap(config);
	if(*error!=nullptr) {
		return nullptr;
	}
	if(ret!=0) {
		*error = cplErrToString(ret);
		return nullptr;
	}
	GDALRasterBandH mbnd = GDALGetMaskBand(GDALGetRasterBand(ds,1));
	if( mbnd == nullptr ) {
		*error = strdup("unknown error");
	}
	return mbnd;
}

GDALDatasetH godalTranslate(char *dstName, GDALDatasetH ds, char **switches, char **error, char **config) {
	godalWrap(error,config);
	GDALTranslateOptions *translateopts = GDALTranslateOptionsNew(switches,nullptr);
	if(*error!=nullptr) {
		godalUnwrap(config);
		GDALTranslateOptionsFree(translateopts);
		return nullptr;
	}
	int usageErr=0;
	GDALDatasetH ret = GDALTranslate(dstName, ds, translateopts, &usageErr);
	GDALTranslateOptionsFree(translateopts);
	godalUnwrap(config);
	if(*error!=nullptr) {
		return nullptr;
	}
	if(ret==nullptr || usageErr!=0) {
		*error=strdup("translate: unknown error");
	}
	return ret;
}

GDALDatasetH godalDatasetWarp(char *dstName, int nSrcCount, GDALDatasetH *srcDS, char **switches, char **error, char **config) {
	godalWrap(error,config);
	GDALWarpAppOptions *warpopts = GDALWarpAppOptionsNew(switches,nullptr);
	if(*error!=nullptr) {
		godalUnwrap(config);
		GDALWarpAppOptionsFree(warpopts);
		return nullptr;
	}
	int usageErr=0;
	GDALDatasetH ret = GDALWarp(dstName, nullptr, nSrcCount, srcDS, warpopts, &usageErr);
	GDALWarpAppOptionsFree(warpopts);
	godalUnwrap(config);
	if(*error!=nullptr) {
		return nullptr;
	}
	if(ret==nullptr || usageErr!=0) {
		*error=strdup("warp: unknown error");
	}
	return ret;
}

char *godalDatasetWarpInto(GDALDatasetH dstDs,  int nSrcCount, GDALDatasetH *srcDS, char **switches, char **config) {
	char *error = nullptr;
	godalWrap(&error, nullptr);
	GDALWarpAppOptions *warpopts = GDALWarpAppOptionsNew(switches,nullptr);
	if(error!=nullptr) {
		godalUnwrap(config);
		GDALWarpAppOptionsFree(warpopts);
		return error;
	}
	int usageErr=0;
	GDALDatasetH ret = GDALWarp(nullptr, dstDs, nSrcCount, srcDS, warpopts, &usageErr);
	GDALWarpAppOptionsFree(warpopts);
	godalUnwrap(config);
	if(error!=nullptr) {
		return error;
	}
	if(ret==nullptr || usageErr!=0) {
		error=strdup("warp: unknown error");
	}
	return nullptr;
}

GDALDatasetH godalDatasetVectorTranslate(char *dstName, GDALDatasetH ds, char **switches, char **error, char **config) {
	godalWrap(error,config);
	GDALVectorTranslateOptions *opts = GDALVectorTranslateOptionsNew(switches,nullptr);
	if(*error!=nullptr) {
		godalUnwrap(config);
		GDALVectorTranslateOptionsFree(opts);
		return nullptr;
	}
	int usageErr=0;
	GDALDatasetH ret = GDALVectorTranslate(dstName, nullptr, 1, &ds, opts, &usageErr);
	GDALVectorTranslateOptionsFree(opts);
	godalUnwrap(config);
	if(*error!=nullptr) {
		return nullptr;
	}
	if(ret==nullptr || usageErr!=0) {
		*error=strdup("ogr2ogr: unknown error");
	}
	return ret;
}

char *godalClearOverviews(GDALDatasetH ds) {
	char *error = nullptr;
	godalWrap(&error, nullptr);
	CPLErr ret = GDALBuildOverviews(ds,"NEAREST",0,nullptr,0,nullptr,nullptr,nullptr);
	godalUnwrap(nullptr);
	if (error != nullptr) {
		return error;
	}
	if (ret != 0) {
		return cplErrToString(ret);
	}
	return nullptr;
}
char *godalBuildOverviews(GDALDatasetH ds, const char *resampling, int nLevels, int *levels,
						  int nBands, int *bands, char **config) {
	char *error = nullptr;
	godalWrap(&error, config);
	CPLErr ret = GDALBuildOverviews(ds,resampling,nLevels,levels,nBands,bands,nullptr,nullptr);
	godalUnwrap(config);
	if (error != nullptr) {
		return error;
	}
	if (ret != 0) {
		return cplErrToString(ret);
	}
	return nullptr;
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

char *godalBandRasterIO(GDALRasterBandH bnd, GDALRWFlag rw, int nDSXOff, int nDSYOff, int nDSXSize, int nDSYSize, void *pBuffer,
		int nBXSize, int nBYSize, GDALDataType eBDataType, int nPixelSpace, int nLineSpace, GDALRIOResampleAlg alg, char **config) {
	char *error = nullptr;
	godalWrap(&error, config);
	GDALRasterIOExtraArg exargs;
	INIT_RASTERIO_EXTRA_ARG(exargs);
	if (alg != GRIORA_NearestNeighbour) {
		exargs.eResampleAlg = alg;
	}
	CPLErr ret = GDALRasterIOEx(bnd, rw, nDSXOff, nDSYOff, nDSXSize, nDSYSize, pBuffer, nBXSize, nBYSize,
									 eBDataType, nPixelSpace, nLineSpace, &exargs);
	godalUnwrap(config);
	if (error != nullptr)
	{
		return error;
	}
	if (ret != 0)
	{
		return cplErrToString(ret);
	}
	return nullptr;
}
char *godalDatasetRasterIO(GDALDatasetH ds, GDALRWFlag rw, int nDSXOff, int nDSYOff, int nDSXSize, int nDSYSize, void *pBuffer,
		int nBXSize, int nBYSize, GDALDataType eBDataType, int nBandCount, int *panBandCount,
		int nPixelSpace, int nLineSpace, int nBandSpace, GDALRIOResampleAlg alg, char **config) {
	char *error = nullptr;
	godalWrap(&error, config);
	GDALRasterIOExtraArg exargs;
	INIT_RASTERIO_EXTRA_ARG(exargs);
	if (alg != GRIORA_NearestNeighbour) {
		exargs.eResampleAlg = alg;
	}
	CPLErr ret = GDALDatasetRasterIOEx(ds, rw, nDSXOff, nDSYOff, nDSXSize, nDSYSize, pBuffer, nBXSize, nBYSize,
									 eBDataType, nBandCount, panBandCount, nPixelSpace, nLineSpace, nBandSpace, &exargs);
	godalUnwrap(config);
	if (error != nullptr) {
		return error;
	}
	if (ret != 0) {
		return cplErrToString(ret);
	}
	return nullptr;
}

char *godalFillRaster(GDALRasterBandH bnd, double real, double imag) {
	char *error = nullptr;
	godalWrap(&error, nullptr);
	CPLErr ret = GDALFillRaster(bnd,real,imag);
	godalUnwrap(nullptr);
	if (error != nullptr) {
		return error;
	}
	if (ret != 0) {
		return cplErrToString(ret);
	}
	return nullptr;

}

char *godalPolygonize(GDALRasterBandH in, GDALRasterBandH mask, OGRLayerH layer,int fieldIndex, char **opts) {
	if (fieldIndex >= OGR_FD_GetFieldCount(OGR_L_GetLayerDefn(layer))) {
		return strdup("invalid fieldIndex");
	}
	char *error = nullptr;
	godalWrap(&error, nullptr);
	CPLErr ret = GDALPolygonize(in,mask,layer,fieldIndex,opts,nullptr,nullptr);
	godalUnwrap(nullptr);
	if (error != nullptr) {
		return error;
	}
	if (ret != 0) {
		return cplErrToString(ret);
	}
	return nullptr;
}

GDALDatasetH godalRasterize(char *dstName, GDALDatasetH ds, char **switches, char **error, char **config) {
	godalWrap(error,config);
	GDALRasterizeOptions *ropts = GDALRasterizeOptionsNew(switches,nullptr);
	if(*error!=nullptr) {
		godalUnwrap(config);
		GDALRasterizeOptionsFree(ropts);
		return nullptr;
	}
	int usageErr=0;
	GDALDatasetH ret = GDALRasterize(dstName, nullptr, ds, ropts, &usageErr);
	GDALRasterizeOptionsFree(ropts);
	godalUnwrap(config);
	if(*error!=nullptr) {
		return nullptr;
	}
	if(ret==nullptr || usageErr!=0) {
		*error=strdup("translate: unknown error");
	}
	return ret;
}

char *godalRasterizeGeometry(GDALDatasetH ds, OGRGeometryH geom, int *bands, int nBands, double *vals, int allTouched) {
	char *error=nullptr;
	const char *opts[2] = { "ALL_TOUCHED=TRUE",nullptr };
	char **copts=(char**)opts;
	if (!allTouched) {
		copts=nullptr;
	}
	godalWrap(&error,nullptr);
	CPLErr gret = GDALRasterizeGeometries(ds,nBands,bands,1,&geom,nullptr,nullptr,vals,copts,nullptr,nullptr);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if (gret != 0) {
		return cplErrToString(gret);
	}
	return nullptr;
}

char *godalLayerDeleteFeature(OGRLayerH layer, OGRFeatureH feat) {
	GIntBig fid = OGR_F_GetFID(feat);
	if (fid == OGRNullFID) {
		return strdup("cannot delete feature with no FID");
	}
	char *error=nullptr;
	godalWrap(&error,nullptr);
	OGRErr gret = OGR_L_DeleteFeature(layer,fid);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if (gret != 0) {
		return ogrErrToString(gret);
	}
	return nullptr;
}

char *godalLayerSetFeature(OGRLayerH layer, OGRFeatureH feat) {
	char *error=nullptr;
	godalWrap(&error,nullptr);
	OGRErr gret = OGR_L_SetFeature(layer,feat);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if (gret != 0) {
		return ogrErrToString(gret);
	}
	return nullptr;
}
char *godalFeatureSetGeometry(OGRFeatureH feat, OGRGeometryH geom) {
	char *error=nullptr;
	godalWrap(&error,nullptr);
	OGRErr gret = OGR_F_SetGeometry(feat,geom);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if (gret != 0) {
		return ogrErrToString(gret);
	}
	return nullptr;
}

OGRGeometryH godal_OGR_G_Simplify(OGRGeometryH in, double tolerance, char **error) {
	godalWrap(error,nullptr);
	OGRGeometryH ret = OGR_G_Simplify(in,tolerance);
	godalUnwrap(nullptr);
	if(*error!=nullptr) {
		return nullptr;
	}
	if(ret==nullptr) {
		*error=strdup("unknown error");
	}
	return ret;
}

OGRGeometryH godal_OGR_G_Buffer(OGRGeometryH in, double tolerance, int segments, char **error) {
	godalWrap(error,nullptr);
	OGRGeometryH ret = OGR_G_Buffer(in,tolerance,segments);
	godalUnwrap(nullptr);
	if(*error!=nullptr) {
		return nullptr;
	}
	if(ret==nullptr) {
		*error=strdup("unknown error");
	}
	return ret;
}

OGRLayerH godalCreateLayer(GDALDatasetH ds, char *name, OGRSpatialReferenceH sr, OGRwkbGeometryType gtype, char **error) {
	godalWrap(error,nullptr);
	OGRLayerH ret = OGR_DS_CreateLayer(ds,name,sr,gtype,nullptr);
	godalUnwrap(nullptr);
	if(*error!=nullptr) {
		return nullptr;
	}
	if(ret==nullptr) {
		*error=strdup("OGR_DS_CreateLayer: unknown error");
	}
	return ret;
}


char *godalLayerFeatureCount(OGRLayerH layer, int *count) {
	char *error=nullptr;
	godalWrap(&error,nullptr);
	GIntBig gcount = OGR_L_GetFeatureCount(layer, 1);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	*count=(int)gcount;
	return nullptr;
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

char* godalSetColorTable(GDALRasterBandH bnd, GDALPaletteInterp interp, int nEntries, short *entries) {
	char *error=nullptr;
	godalWrap(&error,nullptr);
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
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if (gret != 0) {
		return cplErrToString(gret);
	}
	return nullptr;
}

VSILFILE *godalVSIOpen(const char *name, char **error) {
	godalWrap(error,nullptr);
	VSILFILE *fp = VSIFOpenExL(name,"r",1);
	godalUnwrap(nullptr);
	if (*error!=nullptr) {
		if(fp!=nullptr) {
			VSIFCloseL(fp);
		}
		return nullptr;
	}
	if ( fp == nullptr ) {
		*error=strdup("unknown error");
	}
	return fp;
}

char* godalVSIUnlink(const char *fname) {
	char *error=nullptr;
	godalWrap(&error,nullptr);
	int ret = VSIUnlink(fname);
	godalUnwrap(nullptr);
	if(error==nullptr && ret!=0) {
		error = strdup("unkown error");
	}
	return error;
}

char* godalVSIClose(VSILFILE *f) {
	char *error=nullptr;
	godalWrap(&error,nullptr);
	int ret = VSIFCloseL(f);
	godalUnwrap(nullptr);
	if(error==nullptr && ret!=0) {
		error = strdup("unkown error");
	}
	return error;
}

size_t godalVSIRead(VSILFILE *f, void *buf, int len, char **error) {
	godalWrap(error,nullptr);
	size_t read = VSIFReadL(buf,1,len,f);
	godalUnwrap(nullptr);
	return read;
}

char *godalRasterHistogram(GDALRasterBandH bnd, double *min, double *max, int *buckets,
						   unsigned long long **values, int bIncludeOutOfRange, int bApproxOK) {
	char *error = nullptr;
	godalWrap(&error,nullptr);
	CPLErr ret;
	if (*buckets == 0) {
		ret=GDALGetDefaultHistogramEx(bnd,min,max,buckets,values,1,nullptr,nullptr);
	} else {
		*values = (unsigned long long*) VSIMalloc(*buckets*sizeof(GUIntBig));
		ret=GDALGetRasterHistogramEx(bnd,*min,*max,*buckets,*values,bIncludeOutOfRange,bApproxOK,nullptr,nullptr);
	}
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if (ret != 0) {
		return cplErrToString(ret);
	}
	return nullptr;
}
OGRGeometryH godalNewGeometryFromWKT(char *wkt, OGRSpatialReferenceH sr, char **error) {
	godalWrap(error,nullptr);
	OGRGeometryH gptr;
	char **wktPtr = &wkt;
	OGRErr gret = OGR_G_CreateFromWkt(wktPtr,sr,&gptr);
	godalUnwrap(nullptr);
	if (gret != OGRERR_NONE && *error==nullptr) {
		*error = ogrErrToString(gret);
	}
	if (*error!=nullptr) {
		if(gptr!=nullptr) {
			OGR_G_DestroyGeometry(gptr);
		}
		return nullptr;
	}
	if ( gptr == nullptr ) {
		*error=strdup("unknown error");
	}
	return gptr;
}
OGRGeometryH godalNewGeometryFromWKB(void *wkb, int wkbLen, OGRSpatialReferenceH sr, char **error) {
	godalWrap(error,nullptr);
	OGRGeometryH gptr;
	OGRErr gret = OGR_G_CreateFromWkb(wkb,sr,&gptr, wkbLen);
	godalUnwrap(nullptr);
	if (gret != OGRERR_NONE && *error==nullptr) {
		*error = ogrErrToString(gret);
	}
	if (*error!=nullptr) {
		if(gptr!=nullptr) {
			OGR_G_DestroyGeometry(gptr);
		}
		return nullptr;
	}
	if ( gptr == nullptr ) {
		*error=strdup("unknown error");
	}
	return gptr;
}
char* godalExportGeometryWKT(OGRGeometryH in, char **error) {
	godalWrap(error,nullptr);
	char *wkt=nullptr;
	OGRErr gret = OGR_G_ExportToWkt(in,&wkt);
	godalUnwrap(nullptr);
	if (gret != OGRERR_NONE && *error==nullptr) {
		*error = ogrErrToString(gret);
	}
	if (*error!=nullptr) {
		if(wkt!=nullptr) {
			CPLFree(wkt);
		}
		return nullptr;
	}
	if ( wkt == nullptr ) {
		*error=strdup("unknown error");
	}
	return wkt;
}

char* godalExportGeometryWKB(void **wkb, int *wkbLen, OGRGeometryH in) {
	*wkbLen=OGR_G_WkbSize(in);
	if (*wkbLen == 0) {
		*wkb=nullptr;
		return nullptr;
	}
	*wkb = malloc(*wkbLen);
	char *error = nullptr;
	godalWrap(&error,nullptr);
	OGRErr gret = OGR_G_ExportToIsoWkb(in,wkbNDR,(unsigned char*)*wkb);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if (gret != 0) {
		return ogrErrToString(gret);
	}
	return nullptr;
}

char* godalExportGeometryGeoJSON(OGRGeometryH in, int precision, char **error) {
	godalWrap(error,nullptr);
	char* opts[2];
	char precOpt[64];
	snprintf(precOpt,64,"COORDINATE_PRECISION=%d",precision);
	opts[0]=precOpt;
	opts[1]=nullptr;
	char *gj = OGR_G_ExportToJsonEx(in,opts);
	godalUnwrap(nullptr);
	if (gj==nullptr && *error==nullptr) {
		*error=strdup("unknown error");
	}
	if (*error) {
		CPLFree(gj);
		gj=nullptr;
	}
	return gj;
}

char *godalGeometryTransformTo(OGRGeometryH geom, OGRSpatialReferenceH sr) {
	char *error = nullptr;
	godalWrap(&error,nullptr);
	OGRErr gret = OGR_G_TransformTo(geom,sr);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if (gret != 0) {
		return ogrErrToString(gret);
	}
	OGR_G_AssignSpatialReference(geom, sr);
	return nullptr;
}

char *godalGeometryTransform(OGRGeometryH geom, OGRCoordinateTransformationH trn, OGRSpatialReferenceH dst) {
	char *error = nullptr;
	godalWrap(&error,nullptr);
	OGRErr gret = OGR_G_Transform(geom,trn);
	godalUnwrap(nullptr);
	if(error!=nullptr) {
		return error;
	}
	if (gret != 0) {
		return ogrErrToString(gret);
	}
	OGR_G_AssignSpatialReference(geom, dst);
	return nullptr;
}

OGRFeatureH godalLayerNewFeature(OGRLayerH layer, OGRGeometryH geom, char **error) {
	godalWrap(error,nullptr);
	OGRFeatureH hFeature = OGR_F_Create( OGR_L_GetLayerDefn( layer ) );
	OGRErr oe=OGRERR_NONE;
	if (hFeature!=nullptr && geom!=nullptr) {
		oe = OGR_F_SetGeometry(hFeature,geom);
		if (oe != OGRERR_NONE) {
			oe = OGR_L_SetFeature(layer,hFeature);
		}
	}
	godalUnwrap(nullptr);
	if(*error == nullptr && oe!=OGRERR_NONE) {
		*error=ogrErrToString(oe);
	}
	if (hFeature==nullptr && *error==nullptr) {
		*error=strdup("unknown error");
	}
	if (*error!=nullptr && hFeature!=nullptr) {
		OGR_F_Destroy(hFeature);
		hFeature=nullptr;
	}
	return hFeature;
}

GDALDatasetH godalBuildVRT(char *dstName, char **sources, char **switches, char **error, char **config) {
	godalWrap(error,config);
	GDALBuildVRTOptions *ropts = GDALBuildVRTOptionsNew(switches,nullptr);
	if(*error!=nullptr) {
		godalUnwrap(config);
		GDALBuildVRTOptionsFree(ropts);
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
	godalUnwrap(config);
	if(*error!=nullptr) {
		return nullptr;
	}
	if(ret==nullptr || usageErr!=0) {
		*error=strdup("buildvrt: unknown error");
	}
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
                               bool bSetError) override;

        int Stat(const char *pszFilename, VSIStatBufL *pStatBuf, int nFlags) override;
        char **SiblingFiles(const char *pszFilename) override;
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
                                                   bool bSetError)
    {
        if (strchr(pszAccess, 'w') != NULL ||
            strchr(pszAccess, '+') != NULL)
        {
            CPLError(CE_Failure, CPLE_AppDefined, "Only read-only mode is supported");
            return NULL;
        }
        char *err = nullptr;
        long long s = _gogdalSizeCallback((char *)pszFilename, &err);

        if (s == -1)
        {
            if (err != nullptr)
            {
                CPLError(CE_Failure, CPLE_AppDefined, "%s", err);
            }
            errno = ENOENT;
            return NULL;
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

    char **VSIGoFilesystemHandler::SiblingFiles(const char *pszFilename)
    {
        return (char **)calloc(1, sizeof(char *));
    }

} // namespace cpl

char* VSIInstallGoHandler(const char *pszPrefix, size_t bufferSize, size_t cacheSize)
{
    CSLConstList papszPrefix = VSIFileManager::GetPrefixes();
    for( size_t i = 0; papszPrefix && papszPrefix[i]; ++i ) {
        if(strcmp(papszPrefix[i],pszPrefix)==0) {
            return strdup("handler already registered on prefix");
        }
    }
    VSIFilesystemHandler *poHandler = new cpl::VSIGoFilesystemHandler(bufferSize, cacheSize);
    const std::string sPrefix(pszPrefix);
    VSIFileManager::InstallHandler(sPrefix, poHandler);
    return nullptr;
}

