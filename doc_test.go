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
