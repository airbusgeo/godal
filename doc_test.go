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
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"

	"github.com/airbusgeo/godal"
)

func Example() {
	// All godal programs should call this function once at init time to setup
	// gdal's internal drivers.
	godal.RegisterAll()
	// Alternatively, you may also register a select number of drivers by calling one or more of
	//  godal.RegisterInternal()
	//  godal.RegisterRaster(godal.GTiff,godal.VRT)
	//  godal.RegisterVector(godal.Shapefile)
	tmpfile, _ := ioutil.TempFile("", "")
	tmpfile.Close()
	dsfile := tmpfile.Name()
	defer os.Remove(dsfile)

	dataset, err := godal.Create(godal.GTiff, dsfile, 3, godal.Byte, 200, 200,
		godal.CreationOption("TILED=YES", "BLOCKXSIZE=32", "BLOCKYSIZE=32", "COMPRESS=LZW"))
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dsfile, err))
	}
	err = dataset.Close()
	if err != nil {
		panic(fmt.Errorf("close failed: %w", err))
	}

	dataset, err = godal.Open(dsfile)
	if err != nil {
		panic(fmt.Errorf("re-open failed: %w", err))
	}
	//Dataset.Close() must be called exactly once
	defer dataset.Close()

	structure := dataset.Structure()

	fmt.Printf("dataset has %d %s bands. size: w=%d, h=%d", structure.NBands, structure.DataType, structure.SizeX, structure.SizeY)
	// Output: dataset has 3 Byte bands. size: w=200, h=200
}

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
	_ = bands[0].Write(0, 0, buf, 200, 200)

	//add a mask band to the dataset.
	maskBnd, _ := ds.CreateMaskBand(0x02, godal.ConfigOption("GDAL_TIFF_INTERNAL_MASK=YES"))

	//we now want to populate the mask data. we will do this block by block to optimize data access
	structure := bands[0].Structure()

	//allocate a memory buffer that is big enough to contain a whole block
	blockBuf := make([]byte, 32*32)

	//iterate over all blocks
	for block, ok := structure.FirstBlock(), true; ok; block, ok = block.Next() {
		//read the (previously created random data) into our memory buffer
		_ = bands[0].Read(block.X0, block.Y0, blockBuf, block.W, block.H)

		//populate the mask band, by setting to nodata if the pixel value is under 100
		for pix := 0; pix < block.W*block.H; pix++ {
			if blockBuf[pix] < 100 {
				blockBuf[pix] = 0
			} else {
				blockBuf[pix] = 255
			}
		}

		//write the dynamically created mask data into the mask band
		_ = maskBnd.Write(block.X0, block.Y0, blockBuf, block.W, block.H)

	}

	//write dataset to disk
	_ = ds.Close()
}

// This is the godal port of the official gdal raster tutorial
// located at https://gdal.org/tutorials/raster_api_tut.html .
func Example_tutorial1() {
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
		fmt.Printf("Projection is '%s'\n", pj)
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
	poSrcDS, _ := godal.Open("testdata/test.tif")
	pszDstFilename := "/vsimem/tempfile.tif"
	defer godal.VSIUnlink(pszDstFilename)
	//godal doesn't expose createCopy directly, but the same result can be obtained with Translate
	poDstDS, err := poSrcDS.Translate(pszDstFilename, nil, godal.CreationOption("TILED=YES", "COMPRESS=PACKBITS"), godal.GTiff)
	if err != nil {
		panic(err)
	}
	err = poDstDS.Close()
	if err != nil {
		panic(err)
	}
	_ = poSrcDS.Close()

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

	//TODO: deal with error handling
	poDstDS, err = godal.Create(godal.GTiff, pszDstFilename, 1, godal.Byte, 512, 512)
	defer poDstDS.Close() //Close can be defered / called more than once (second+ calls are no-ops)

	err = poDstDS.SetGeoTransform([6]float64{444720, 30, 0, 3751320, 0, -30})

	//SetUTM and SetWellKnownGeogCS not implemented. godal allows populating
	// a SpatialRef from a WKT or PROJ4 string, or an epsg code
	sr, err := godal.NewSpatialRefFromEPSG(4326)
	defer sr.Close()
	err = poDstDS.SetSpatialRef(sr)

	abyRaster := make([]byte, 512*512)
	// ... now populate with data
	err = poDstDS.Bands()[0].Write(0, 0, abyRaster, 512, 512)
	err = poDstDS.Close()

	// Output:
	// Size is 10x10x3
	// Projection is 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NORTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]'
	// Origin = (45.000000,35.000000)
	// Pixel Size = (1.000000,-1.000000)
	// Block=256x256 Type=Byte, ColorInterp=Red
	// Driver GTiff supports Create() method.
	// Driver Gtiff supports CreateCopy() method.
}
