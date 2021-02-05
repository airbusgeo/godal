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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyDataset(t *testing.T) {
	//These tests are essentially here to cover error cases
	tmpdir, _ := ioutil.TempDir("", "")
	fname := filepath.Join(tmpdir, "ff.tif")
	defer func() {
		_ = os.Chmod(tmpdir, 0777)
		_ = os.Chmod(fname, 0777)
		_ = os.RemoveAll(tmpdir)
	}()
	ds, _ := Open("testdata/test.tif")
	rds, _ := ds.Translate(fname, []string{"-b", "1", "-b", "1", "-of", "GTiff"})
	err := rds.Bands()[0].SetColorInterp(CIAlpha)
	assert.NoError(t, err)
	//Setting a second alpha band on a gtiff raises a warning
	err = rds.Bands()[1].SetColorInterp(CIAlpha)
	assert.Error(t, err)
	_ = rds.Close()
	_ = ds.Close()
	_ = os.Chmod(fname, 0400)
	ds, err = Open(fname)
	require.NoError(t, err)
	_ = os.Chmod(tmpdir, 0400)
	err = ds.SetGeoTransform([6]float64{2, 3, 4, 5, 6, 7})
	assert.Error(t, err)
	/* gdal does not raise a read-only error for these :(
	err = ds.SetMetadata("foo", "baz")
	if err == nil {
		t.Error("set metadata")
	}
	epsg4326, _ := NewSpatialRefFromEPSG(4326)
	err = ds.SetSpatialRef(epsg4326)
	if err == nil {
		t.Error("set projection")
	}
	err = ds.Bands()[0].ClearNoData()
	if err == nil {
		t.Error("ro clear nodata")
	}
	err = ds.Bands()[0].SetColorInterp(CI_CyanBand)
	if err == nil {
		t.Error("ro colorinterp")
	}
	err = ds.SetNoData(34)
	if err == nil {
		t.Error("set ro nodata")
	}
	*/
	_, err = ds.CreateMaskBand(0x02, ConfigOption("GDAL_TIFF_INTERNAL_MASK=YES"))
	if err == nil {
		t.Error("create mask")
	}
	_, err = ds.Bands()[0].CreateMask(0x02, ConfigOption("GDAL_TIFF_INTERNAL_MASK=YES"))
	if err == nil {
		t.Error("create mask")
	}
	err = ds.Bands()[0].Fill(5, 5)
	if err == nil {
		t.Error("fill ro")
	}

}
func TestDatasetRead(t *testing.T) {

	ds, err := Create(Memory, "", 3, Byte, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	bnds := ds.Bands()
	for i := range bnds {
		err = bnds[i].Fill(float64(10*i), 0)
		if err != nil {
			t.Error(err)
		}
	}

	buf := make([]byte, 300)

	err = ds.Read(95, 95, buf, 10, 10)
	if err == nil {
		t.Error("error not raised")
	}
	err = ds.Read(100, 100, buf, 10, 10)
	if err == nil {
		t.Error("error not raised")
	}
	err = ds.Read(0, 0, buf, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0 || buf[1] != 10 || buf[2] != 20 {
		t.Errorf("vals: %d %d %d", buf[0], buf[1], buf[2])
	}

	_ = ds.Read(0, 0, buf, 10, 10, Bands(1, 2), Window(5, 5))
	if buf[0] != 10 || buf[1] != 20 {
		t.Errorf("vals: %d %d", buf[0], buf[1])
	}
	_ = ds.Read(0, 0, buf, 10, 10, Bands(1, 2), BandInterleaved())
	if buf[0] != 10 || buf[99] != 10 || buf[100] != 20 || buf[101] != 20 {
		t.Errorf("vals: %d %d %d %d", buf[0], buf[99], buf[100], buf[101])
	}

	fbuf := make([]float64, 200)
	err = ds.Read(0, 0, fbuf, 10, 10, Bands(0, 1))
	if err != nil {
		t.Error(err)
	}
	if fbuf[0] != 0 || fbuf[1] != 10 || fbuf[199] != 10 {
		t.Errorf("%f %f %f", fbuf[0], fbuf[1], fbuf[199])
	}
}

func TestCastedIO(t *testing.T) {
	ds, _ := Create(Memory, "", 3, Byte, 10, 10)
	data := make([]float64, 300)
	for i := range data {
		data[i] = float64(i / 3)
	}
	clear := func() {
		for i := range data {
			data[i] = float64(i / 3)
		}
	}
	err := ds.Write(0, 0, data, 10, 10)
	if err != nil {
		t.Error(err)
	}
	clear()
	err = ds.Read(0, 0, data, 10, 10)
	if err != nil {
		t.Error(err)
	}
	for i := range data {
		if data[i] != float64(i/3) {
			t.Errorf("pix %d: got %f expected %f", i, data[i], float64(i/3))
		}
	}
}

func TestBandRead(t *testing.T) {
	ds, err := Create(Memory, "", 1, Byte, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	bnd := ds.Bands()[0]
	buf := make([]byte, 100)
	for x := 0; x < 10; x++ {
		for y := 0; y < 10; y++ {
			if x > y {
				buf[y*10+x] = byte(x)
			} else {
				buf[y*10+x] = byte(y)
			}
		}
	}
	err = bnd.Write(0, 0, buf, 10, 10)
	if err != nil {
		t.Error(err)
	}

	err = bnd.Read(95, 95, buf, 10, 10)
	if err == nil {
		t.Error("error not raised")
	}
	err = bnd.Read(100, 100, buf, 10, 10)
	if err == nil {
		t.Error("error not raised")
	}
	err = bnd.Read(0, 0, buf, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	if buf[0] != 0 || buf[99] != 9 {
		t.Errorf("vals: %v", buf)
	}

	_ = bnd.Read(0, 0, buf, 10, 10, Window(5, 5))
	if buf[0] != 0 || buf[99] != 4 {
		t.Errorf("vals: %v", buf)
	}
	fbuf := make([]float64, 100)
	err = bnd.Read(0, 0, fbuf, 10, 10)
	if err != nil {
		t.Error(err)
	}
	if fbuf[0] != 0 || fbuf[99] != 9 {
		t.Errorf("%v", fbuf)
	}
}

func TestStridedIO(t *testing.T) {
	ds, _ := Create(Memory, "", 3, Byte, 8, 8)
	padData := make([]byte, 8*16*3)
	for i := 0; i < 16; i++ {
		for j := 0; j < 8; j++ {
			padData[j*48+i*3] = uint8(i)
			padData[j*48+i*3+1] = uint8(i + 16)
			padData[j*48+i*3+2] = uint8(i + 32)
		}
	}
	err := ds.Write(0, 0, padData, 8, 8, LineSpacing(16*3))
	if err != nil {
		t.Error(err)
	}
	for i := range padData {
		padData[i] = 0
	}
	_ = ds.Read(0, 0, padData, 8, 8)
	for i := 0; i < 8; i++ {
		for j := 0; j < 8; j++ {
			if padData[j*24+i*3] != uint8(i) {
				t.Errorf("pix %d,%d: got %d expected %d", i, j, padData[j*24+i*3], i)
			}
			if padData[j*24+i*3+1] != uint8(i+16) {
				t.Errorf("pix %d,%d: got %d expected %d", i, j, padData[j*24+i*3+1], i+16)
			}
			if padData[j*24+i*3+2] != uint8(i+32) {
				t.Errorf("pix %d,%d: got %d expected %d", i, j, padData[j*24+i*3+2], i+32)
			}
		}
	}
	for i := range padData {
		padData[i] = 0
	}
	_ = ds.Read(0, 0, padData, 8, 8, BandInterleaved())
	for i := 0; i < 8; i++ {
		for j := 0; j < 8; j++ {
			if padData[j*8+i] != uint8(i) {
				t.Errorf("pix 0 %d,%d: got %d expected %d", i, j, padData[j*8+i], i)
			}
			if padData[64+j*8+i] != uint8(i+16) {
				t.Errorf("pix 1 %d,%d: got %d expected %d", i, j, padData[64+j*8+i], i+16)
			}
			if padData[128+j*8+i] != uint8(i+32) {
				t.Errorf("pix 2 %d,%d: got %d expected %d", i, j, padData[128+j*8+i], i+32)
			}
		}
	}
	for i := range padData {
		padData[i] = 0
	}
	//same as bandinterleaved, instead hard-coded
	_ = ds.Read(0, 0, padData, 8, 8, BandSpacing(64), PixelSpacing(1), LineSpacing(8))
	for i := 0; i < 8; i++ {
		for j := 0; j < 8; j++ {
			if padData[j*8+i] != uint8(i) {
				t.Errorf("pix 0 %d,%d: got %d expected %d", i, j, padData[j*8+i], i)
			}
			if padData[64+j*8+i] != uint8(i+16) {
				t.Errorf("pix 1 %d,%d: got %d expected %d", i, j, padData[64+j*8+i], i+16)
			}
			if padData[128+j*8+i] != uint8(i+32) {
				t.Errorf("pix 2 %d,%d: got %d expected %d", i, j, padData[128+j*8+i], i+32)
			}
		}
	}
	for i := range padData {
		padData[i] = 0
	}
	_ = ds.Read(0, 0, padData, 8, 8, Bands(0, 2))
	for i := 0; i < 8; i++ {
		for j := 0; j < 8; j++ {
			if padData[j*16+i*2] != uint8(i) {
				t.Errorf("pix 0 %d,%d: got %d expected %d", i, j, padData[j*16+i*2], i)
			}
			if padData[j*16+i*2+1] != uint8(i+32) {
				t.Errorf("pix 2 %d,%d: got %d expected %d", i, j, padData[j*16+i*2+1], i+32)
			}
		}
	}
	for i := range padData {
		padData[i] = 0
	}
	_ = ds.Read(0, 0, padData, 8, 8, BandInterleaved(), Bands(0, 2))
	for i := 0; i < 8; i++ {
		for j := 0; j < 8; j++ {
			if padData[j*8+i] != uint8(i) {
				t.Errorf("pix %d,%d: got %d expected %d", i, j, padData[j*8+i], i)
			}
			if padData[64+j*8+i] != uint8(i+32) {
				t.Errorf("pix %d,%d: got %d expected %d", i, j, padData[64+j*8+i], i+32)
			}
		}
	}
	for i := range padData {
		padData[i] = 0
	}
	_ = ds.Bands()[0].Read(0, 0, padData, 8, 8, LineSpacing(10))
	for i := 0; i < 8; i++ {
		for j := 0; j < 8; j++ {
			if padData[j*10+i] != uint8(i) {
				t.Errorf("pix %d,%d: got %d expected %d", i, j, padData[j*10+i], i)
			}
		}
	}
	for i := range padData {
		padData[i] = 0
	}
	_ = ds.Bands()[0].Read(0, 0, padData, 8, 8, PixelSpacing(2))
	for i := 0; i < 8; i++ {
		for j := 0; j < 8; j++ {
			if padData[j*16+i*2] != uint8(i) {
				t.Errorf("pix %d,%d: got %d expected %d", i, j, padData[j*16+i*2], i)
			}
			if padData[j*16+i*2+1] != 0 {
				t.Errorf("pix+1 %d,%d: got %d expected %d", i, j, padData[j*16+i*2+1], 0)
			}
		}
	}
	for i := range padData {
		padData[i] = 0
	}
	_ = ds.Bands()[0].Read(0, 0, padData, 8, 8, PixelSpacing(2), LineSpacing(18))
	for i := 0; i < 8; i++ {
		for j := 0; j < 8; j++ {
			if padData[j*18+i*2] != uint8(i) {
				t.Errorf("pix %d,%d: got %d expected %d", i, j, padData[j*18+i*2], i)
			}
			if padData[j*18+i*2+1] != 0 {
				t.Errorf("pix+1 %d,%d: got %d expected %d", i, j, padData[j*18+i*2+1], 0)
			}
		}
	}
}

func TestBlockIterator(t *testing.T) {
	tmpname := tempfile()
	defer os.Remove(tmpname)

	ds, err := Create(GTiff, tmpname, 1, Byte, 63, 65, CreationOption("TILED=YES", "BLOCKXSIZE=32", "BLOCKYSIZE=32"))
	if err != nil {
		t.Fatal(err)
	}
	ibl := 0
	for bl, ok := ds.Structure().FirstBlock(), true; ok; bl, ok = bl.Next() {
		expc := 0
		switch ibl {
		case 0, 2, 4:
			expc = 0
		case 1, 3, 5:
			expc = 32
		default:
			t.Errorf("block %d reached", ibl)
		}
		assert.Equal(t, expc, bl.X0, "block %d x=%d", ibl, bl.X0)
		expc = 0
		switch ibl {
		case 0, 1:
			expc = 0
		case 2, 3:
			expc = 32
		case 4, 5:
			expc = 64
		default:
			t.Errorf("block %d reached", ibl)
		}
		assert.Equal(t, expc, bl.Y0, "block %d y=%d", ibl, bl.Y0)

		expc = 0
		switch ibl {
		case 0, 2, 4:
			expc = 32
		case 1, 3, 5:
			expc = 31
		default:
			t.Errorf("block %d reached", ibl)
		}
		assert.Equal(t, expc, bl.W, "block %d w=%d", ibl, bl.W)
		expc = 0
		switch ibl {
		case 0, 1, 2, 3:
			expc = 32
		case 4, 5:
			expc = 1
		default:
			t.Errorf("block %d reached", ibl)
		}
		assert.Equal(t, expc, bl.H, "block %d w=%d", ibl, bl.H)

		ibl++

	}
}
