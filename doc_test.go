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

package godal_test

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"

	"github.com/airbusgeo/godal"
)

func ExampleBand_IO() {
	tmpfile, _ := ioutil.TempFile("", "")
	tmpfile.Close()
	dsfile := tmpfile.Name()
	defer os.Remove(dsfile)

	//create a 200x200 one band image, internally tiled with 32x32 blocks
	ds, _ := godal.Create(godal.GTiff, dsfile, 1, godal.Byte, 200, 200, godal.CreationOption("TILED=YES", "BLOCKXSIZE=32", "BLOCKYSIZE=32"))

	//fill the band with random data
	buf := make([]byte, 200*200)
	for i := range buf {
		buf[i] = byte(rand.Intn(255))
	}
	bands := ds.Bands()

	//write the random data to the first band
	bands[0].Write(0, 0, buf, 200, 200)

	//add a mask band to the dataset.
	maskBnd, _ := ds.CreateMaskBand(0x02, godal.ConfigOption("GDAL_TIFF_INTERNAL_MASK=YES"))

	//we now want to populate the mask data. we will do this block by block to optimize data access
	structure := bands[0].Structure()

	//allocate a memory buffer that is big enough to contain a whole block
	blockBuf := make([]byte, 32*32)

	//iterate over all blocks
	for block, ok := structure.FirstBlock(), true; ok; block, ok = block.Next() {
		//read the (previously created random data) into our memory buffer
		bands[0].Read(block.X0, block.Y0, blockBuf, block.W, block.H)

		//populate the mask band, by setting to nodata if the pixel value is under 100
		for pix := 0; pix < block.W*block.H; pix++ {
			if blockBuf[pix] < 100 {
				blockBuf[pix] = 0
			} else {
				blockBuf[pix] = 255
			}
		}

		//write the dynamically created mask data into the mask band
		maskBnd.Write(block.X0, block.Y0, blockBuf, block.W, block.H)

	}

	//write dataset to disk
	ds.Close()
}

// This is the godal port of the official gdal raster tutorial
// located at https://gdal.org/tutorials/raster_api_tut.html .
func Example_rasterTutorial() {
	/*
		GDALDatasetH  hDataset;
		GDALAllRegister();
		hDataset = GDALOpen( pszFilename, GA_ReadOnly );
		if( hDataset == NULL )
		{
			...;
		}
	*/
	godal.RegisterAll()
	hDataset, err := godal.Open("testdata/test.tif")
	if err != nil {
		panic(err)
	}

	/*
		hDriver = GDALGetDatasetDriver( hDataset );
		printf( "Driver: %s/%s\n",
			GDALGetDriverShortName( hDriver ),
			GDALGetDriverLongName( hDriver ) );
	*/
	//not implemented

	/*
		printf( "Size is %dx%dx%d\n",
			GDALGetRasterXSize( hDataset ),
			GDALGetRasterYSize( hDataset ),
			GDALGetRasterCount( hDataset ) );
	*/
	structure := hDataset.Structure()
	fmt.Printf("Size is %dx%dx%d\n", structure.SizeX, structure.SizeY, structure.NBands)

	/*
		if( GDALGetProjectionRef( hDataset ) != NULL )
			printf( "Projection is '%s'\n", GDALGetProjectionRef( hDataset ) );
	*/
	if pj := hDataset.Projection(); pj != "" {
		fmt.Printf("Projection is '%s...'\n", pj[0:20])
	}

	/*
		if( GDALGetGeoTransform( hDataset, adfGeoTransform ) == CE_None )
		{
			printf( "Origin = (%.6f,%.6f)\n",
				adfGeoTransform[0], adfGeoTransform[3] );
			printf( "Pixel Size = (%.6f,%.6f)\n",
				adfGeoTransform[1], adfGeoTransform[5] );
		}
	*/
	if gt, err := hDataset.GeoTransform(); err == nil {
		fmt.Printf("Origin = (%.6f,%.6f)\n", gt[0], gt[3])
		fmt.Printf("Pixel Size = (%.6f,%.6f)\n", gt[1], gt[5])
	}

	/*
		GDALRasterBandH hBand;
		int             nBlockXSize, nBlockYSize;
		int             bGotMin, bGotMax;
		double          adfMinMax[2];
		hBand = GDALGetRasterBand( hDataset, 1 );
		GDALGetBlockSize( hBand, &nBlockXSize, &nBlockYSize );
		printf( "Block=%dx%d Type=%s, ColorInterp=%s\n",
				nBlockXSize, nBlockYSize,
				GDALGetDataTypeName(GDALGetRasterDataType(hBand)),
				GDALGetColorInterpretationName(
					GDALGetRasterColorInterpretation(hBand)) );
	*/
	band := hDataset.Bands()[0] //Note that in godal, bands are indexed starting from 0, not 1
	bandStructure := band.Structure()
	fmt.Printf("Block=%dx%d Type=%s, ColorInterp=%s\n",
		bandStructure.BlockSizeX, bandStructure.BlockSizeY,
		bandStructure.DataType,
		band.ColorInterp().Name())

	/*
		adfMinMax[0] = GDALGetRasterMinimum( hBand, &bGotMin );
		adfMinMax[1] = GDALGetRasterMaximum( hBand, &bGotMax );
		if( ! (bGotMin && bGotMax) )
			GDALComputeRasterMinMax( hBand, TRUE, adfMinMax );
		printf( "Min=%.3fd, Max=%.3f\n", adfMinMax[0], adfMinMax[1] );
	*/
	//not implemented

	/*
		if( GDALGetOverviewCount(hBand) > 0 )
			printf( "Band has %d overviews.\n", GDALGetOverviewCount(hBand));
	*/
	if overviews := band.Overviews(); len(overviews) > 0 {
		fmt.Printf("Band has %d overviews.\n", len(overviews))
	}

	/*
		if( GDALGetRasterColorTable( hBand ) != NULL )
			printf( "Band has a color table with %d entries.\n",
					GDALGetColorEntryCount(
						GDALGetRasterColorTable( hBand ) ) );
	*/
	if ct := band.ColorTable(); len(ct.Entries) > 0 {
		fmt.Printf("Band has a color table with %d entries.\n", len(ct.Entries))
	}

	/*
		float *pafScanline;
		int   nXSize = GDALGetRasterBandXSize( hBand );
		pafScanline = (float *) CPLMalloc(sizeof(float)*nXSize);
		GDALRasterIO( hBand, GF_Read, 0, 0, nXSize, 1,
			pafScanline, nXSize, 1, GDT_Float32,
			0, 0 );
	*/

	pafScanline := make([]float32, structure.SizeX)
	err = band.Read(0, 0, pafScanline, bandStructure.SizeX, 1)
	if err != nil {
		panic(err)
	}

	err = hDataset.Close()
	// we don't really need to check for errors here as we have a read-only dataset.
	if err != nil {
		panic(err)
	}

	/*
		const char *pszFormat = "GTiff";
		GDALDriverH hDriver = GDALGetDriverByName( pszFormat );
		char **papszMetadata;
		if( hDriver == NULL )
		    exit( 1 );
		papszMetadata = GDALGetMetadata( hDriver, NULL );
		if( CSLFetchBoolean( papszMetadata, GDAL_DCAP_CREATE, FALSE ) )
		    printf( "Driver %s supports Create() method.\n", pszFormat );
		if( CSLFetchBoolean( papszMetadata, GDAL_DCAP_CREATECOPY, FALSE ) )
		    printf( "Driver %s supports CreateCopy() method.\n", pszFormat );
	*/

	hDriver, ok := godal.RasterDriver("Gtiff")
	if !ok {
		panic("Gtiff not found")
	}
	md := hDriver.Metadatas()
	if md["DCAP_CREATE"] == "YES" {
		fmt.Printf("Driver GTiff supports Create() method.\n")
	}
	if md["DCAP_CREATECOPY"] == "YES" {
		fmt.Printf("Driver Gtiff supports CreateCopy() method.\n")
	}

	/*	GDALDataset *poSrcDS = (GDALDataset *) GDALOpen( pszSrcFilename, GA_ReadOnly );
		GDALDataset *poDstDS;
		char **papszOptions = NULL;
		papszOptions = CSLSetNameValue( papszOptions, "TILED", "YES" );
		papszOptions = CSLSetNameValue( papszOptions, "COMPRESS", "PACKBITS" );
		poDstDS = poDriver->CreateCopy( pszDstFilename, poSrcDS, FALSE,
										papszOptions, GDALTermProgress, NULL );
		if( poDstDS != NULL )
			GDALClose( (GDALDatasetH) poDstDS );
		CSLDestroy( papszOptions );

		GDALClose( (GDALDatasetH) poSrcDS );
	*/

	//Left out: dealing with error handling
	poSrcDS, _ := godal.Open("testdata/test.tif")
	pszDstFilename := "/vsimem/tempfile.tif"
	defer godal.VSIUnlink(pszDstFilename)
	//godal doesn't expose createCopy directly, but the same result can be obtained with Translate
	poDstDS, _ := poSrcDS.Translate(pszDstFilename, nil, godal.CreationOption("TILED=YES", "COMPRESS=PACKBITS"), godal.GTiff)
	poDstDS.Close()
	poSrcDS.Close()

	/*
		GDALDataset *poDstDS;
		char **papszOptions = NULL;
		poDstDS = poDriver->Create( pszDstFilename, 512, 512, 1, GDT_Byte,
									papszOptions );
		double adfGeoTransform[6] = { 444720, 30, 0, 3751320, 0, -30 };
		OGRSpatialReference oSRS;
		char *pszSRS_WKT = NULL;
		GDALRasterBand *poBand;
		GByte abyRaster[512*512];
		poDstDS->SetGeoTransform( adfGeoTransform );
		oSRS.SetUTM( 11, TRUE );
		oSRS.SetWellKnownGeogCS( "NAD27" );
		oSRS.exportToWkt( &pszSRS_WKT );
		poDstDS->SetProjection( pszSRS_WKT );
		CPLFree( pszSRS_WKT );
		poBand = poDstDS->GetRasterBand(1);
		poBand->RasterIO( GF_Write, 0, 0, 512, 512,
						abyRaster, 512, 512, GDT_Byte, 0, 0 );
		GDALClose( (GDALDatasetH) poDstDS );
	*/

	poDstDS, _ = godal.Create(godal.GTiff, pszDstFilename, 1, godal.Byte, 512, 512)
	defer poDstDS.Close() //Close can be defered / called more than once (second+ calls are no-ops)

	poDstDS.SetGeoTransform([6]float64{444720, 30, 0, 3751320, 0, -30})

	//SetUTM and SetWellKnownGeogCS not implemented. godal allows populating
	// a SpatialRef from a WKT or PROJ4 string, or an epsg code
	sr, _ := godal.NewSpatialRefFromEPSG(4326)
	defer sr.Close()
	poDstDS.SetSpatialRef(sr)

	abyRaster := make([]byte, 512*512)
	// ... now populate with data
	poDstDS.Bands()[0].Write(0, 0, abyRaster, 512, 512)
	poDstDS.Close()

	// Output:
	// Size is 10x10x3
	// Projection is 'GEOGCS["WGS 84",DATU...'
	// Origin = (45.000000,35.000000)
	// Pixel Size = (1.000000,-1.000000)
	// Block=256x256 Type=Byte, ColorInterp=Red
	// Driver GTiff supports Create() method.
	// Driver Gtiff supports CreateCopy() method.
}

// This is the godal port of the official gdal vector tutorial
// located at https://gdal.org/tutorials/vector_api_tut.html.
//
// Vector support in godal is incomplete and should be considered a
// work in progress. The API may change in backwards incompatible ways.
func Example_vectorTutorial() {
	/*
		#include "gdal.h"

		int main() {
			GDALAllRegister();
	*/
	godal.RegisterAll()
	/*
		GDALDatasetH hDS;
		OGRLayerH hLayer;
		OGRFeatureH hFeature;
		OGRFeatureDefnH hFDefn;

		hDS = GDALOpenEx( "point.shp", GDAL_OF_VECTOR, NULL, NULL, NULL );
		if( hDS == NULL ) {
			printf( "Open failed.\n" );
			exit( 1 );
		}
	*/

	//by using the VectorOnly() option Open() will return an error if given
	//a raster dataset
	hDS, err := godal.Open("testdata/test.geojson", godal.VectorOnly())
	if err != nil {
		panic(err)
	}
	/*
		hLayer = GDALDatasetGetLayerByName( hDS, "point" );
		hFDefn = OGR_L_GetLayerDefn(hLayer);
		OGR_L_ResetReading(hLayer);
		while( (hFeature = OGR_L_GetNextFeature(hLayer)) != NULL ) {
	*/
	layers := hDS.Layers()
	for _, layer := range layers {
		layer.ResetReading()
		for {
			/*
				int iField;
				OGRGeometryH hGeometry;
				for( iField = 0; iField < OGR_FD_GetFieldCount(hFDefn); iField++ ) {
					OGRFieldDefnH hFieldDefn = OGR_FD_GetFieldDefn( hFDefn, iField );
					switch( OGR_Fld_GetType(hFieldDefn) ) {
					case OFTInteger:
						printf( "%d,", OGR_F_GetFieldAsInteger( hFeature, iField ) );
						break;
					case OFTInteger64:
						printf( CPL_FRMT_GIB ",", OGR_F_GetFieldAsInteger64( hFeature, iField ) );
						break;
					case OFTReal:
						printf( "%.3f,", OGR_F_GetFieldAsDouble( hFeature, iField) );
						break;
					case OFTString:
						printf( "%s,", OGR_F_GetFieldAsString( hFeature, iField) );
						break;
					default:
						printf( "%s,", OGR_F_GetFieldAsString( hFeature, iField) );
						break;
					}
				}
			*/
			feat := layer.NextFeature()
			if feat == nil {
				break
			}
			fields := feat.Fields()
			fmt.Printf("%v\n", fields)

			/*
				hGeometry = OGR_F_GetGeometryRef(hFeature);
				if( hGeometry != NULL
					&& wkbFlatten(OGR_G_GetGeometryType(hGeometry)) == wkbPoint )
					printf( "%.3f,%3.f\n", OGR_G_GetX(hGeometry, 0), OGR_G_GetY(hGeometry, 0) );
				else
					printf( "no point geometry\n" );
			*/
			geom := feat.Geometry()
			wkt, _ := geom.WKT()
			fmt.Printf("geom: %s\n", wkt)

			/*
					OGR_F_Destroy( hFeature );
				}
			*/
			//geom.Close is a no-op in this case. We call it nonetheless, as it is strongly recommended
			//to call Close on an object that implements the method to avoid potential memory leaks.
			geom.Close()

			//calling feat.Close is mandatory to prevent memory leaks
			feat.Close()
		}
	}
	/*
			GDALClose( hDS );
		}
	*/
	hDS.Close()

	/*
		const char *pszDriverName = "ESRI Shapefile";
		GDALDriverH hDriver;
		GDALDatasetH hDS;
		OGRLayerH hLayer;
		OGRFieldDefnH hFieldDefn;
		double x, y;
		char szName[33];

		GDALAllRegister();

		hDriver = GDALGetDriverByName( pszDriverName );
		if( hDriver == NULL )
		{
			printf( "%s driver not available.\n", pszDriverName );
			exit( 1 );
		}

		hDS = GDALCreate( hDriver, "point_out.shp", 0, 0, 0, GDT_Unknown, NULL );
		if( hDS == NULL )
		{
			printf( "Creation of output file failed.\n" );
			exit( 1 );
		}
	*/
	hDS, err = godal.CreateVector(godal.GeoJSON, "/vsimem/point_out.geojson")
	if err != nil {
		panic(err)
	}
	defer godal.VSIUnlink("/vsimem/point_out.geojson")

	/*
	   hLayer = GDALDatasetCreateLayer( hDS, "point_out", NULL, wkbPoint, NULL );
	   if( hLayer == NULL )
	   {
	       printf( "Layer creation failed.\n" );
	       exit( 1 );
	   }

	   hFieldDefn = OGR_Fld_Create( "Name", OFTString );

	   OGR_Fld_SetWidth( hFieldDefn, 32);

	   if( OGR_L_CreateField( hLayer, hFieldDefn, TRUE ) != OGRERR_NONE )
	   {
	       printf( "Creating Name field failed.\n" );
	       exit( 1 );
	   }

	   OGR_Fld_Destroy(hFieldDefn);
	*/

	layer, err := hDS.CreateLayer("point_out", nil, godal.GTPoint,
		godal.NewFieldDefinition("Name", godal.FTString))
	if err != nil {
		panic(fmt.Errorf("Layer creation failed: %w", err))
	}

	/*
	   while( !feof(stdin)
	       && fscanf( stdin, "%lf,%lf,%32s", &x, &y, szName ) == 3 )
	   {
	       OGRFeatureH hFeature;
	       OGRGeometryH hPt;

	       hFeature = OGR_F_Create( OGR_L_GetLayerDefn( hLayer ) );
	       OGR_F_SetFieldString( hFeature, OGR_F_GetFieldIndex(hFeature, "Name"), szName );

	       hPt = OGR_G_CreateGeometry(wkbPoint);
	       OGR_G_SetPoint_2D(hPt, 0, x, y);

	       OGR_F_SetGeometry( hFeature, hPt );
	       OGR_G_DestroyGeometry(hPt);

	       if( OGR_L_CreateFeature( hLayer, hFeature ) != OGRERR_NONE )
	       {
	       printf( "Failed to create feature in shapefile.\n" );
	       exit( 1 );
	       }

	       OGR_F_Destroy( hFeature );
	   }
	*/
	//scanner := bufio.NewScanner(os.Stdin)
	scanner := bufio.NewScanner(strings.NewReader(`POINT (1 1)`))
	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		geom, _ := godal.NewGeometryFromWKT(scanner.Text(), nil)
		feat, err := layer.NewFeature(geom)
		//godal does not yet support setting fields on newly created features
		if err != nil {
			panic(fmt.Errorf("Failed to create feature in shapefile: %w", err))
		}
		gj, _ := feat.Geometry().GeoJSON()
		fmt.Printf("created geometry %s\n", gj)

		feat.Close()
	}
	/*

	   GDALClose( hDS );
	*/
	err = hDS.Close() //Close must be called and the error must be checked when writing
	if err != nil {
		panic(fmt.Errorf("failed to close shapefile: %w", err))
	}

	// Output:
	// map[foo:bar]
	// geom: POLYGON ((100 0,101 0,101 1,100 1,100 0))
	// map[foo:baz]
	// geom: POLYGON ((100 0,101 0,101 1,100 1,100 0))
	// created geometry { "type": "Point", "coordinates": [ 1.0, 1.0 ] }
}

//ExampleErrorHandler_sentinel is an example to make godal.Open return a specific golang
//error when the gdal emitted error/log matches certain criteria
func ExampleErrorHandler_sentinel() {
	sentinel := errors.New("noent")
	eh := func(ec godal.ErrorCategory, code int, msg string) error {
		/* do some advanced checking of ec, code and msg to determine if this is an actual error */
		haveError := true
		if !haveError {
			log.Println(msg)
			return nil
		}
		return sentinel
	}
	_, err := godal.Open("nonexistent.tif", godal.ErrLogger(eh))
	if errors.Is(err, sentinel) {
		fmt.Println(err.Error())
	}

	// Output:
	// noent
}

//ExampleErrorHandler_warnings is an example to set up an error handler that ignores gdal warnings
func ExampleErrorHandler_warnings() {
	eh := func(ec godal.ErrorCategory, code int, msg string) error {
		if ec <= godal.CE_Warning {
			log.Println(msg)
			return nil
		}
		return fmt.Errorf("GDAL %d: %s", code, msg)
	}
	_, err := godal.Open("nonexistent.tif", godal.ErrLogger(eh))
	// err if returned will not arise from a gdal warning
	_ = err
}
