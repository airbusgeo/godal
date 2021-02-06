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
)

func TestOpen(t *testing.T) {
	_, err := Open("testdata/test.tif", Drivers("MEM"))
	if err == nil {
		t.Error("error not raised")
	}
	_, err = Open("testdata/test.tif", DriverOpenOption("bogus=set"))
	if err == nil {
		t.Error("error not raised")
	}
	_, err = Open("testdata/test.tif", VectorOnly())
	if err == nil {
		t.Error("error not raised")
	}
	_, err = Open("testdata/test.geojson", RasterOnly())
	if err == nil {
		t.Error("error not raised")
	}
	ds, err := Open("testdata/test.tif")
	if err != nil {
		t.Fatal(err)
	}
	err = ds.Close()
	if err != nil {
		t.Error(err)
	}

	err = ds.Close()
	if err == nil {
		t.Error("second close did not panic")
	}

	_, err = Open("notexist.tif")
	if err == nil {
		t.Error("error not caught")
	}
	_, err = Open("godal.cpp")
	if err == nil {
		t.Error("error not caught")
	}
}

func TestOpenUpdate(t *testing.T) {
	tt := tempfile()
	defer os.Remove(tt)
	defer os.Remove(tt + ".msk")
	ods, _ := Open("testdata/test.tif")
	uds, _ := ods.Translate(tt, []string{"-of", "GTiff"})
	_ = ods.Close()
	_ = uds.Close()
	uds, err := Open(tt, Update())
	if err != nil {
		t.Fatal(err)
	}
	err = uds.Bands()[0].SetNoData(5)
	if err == nil {
		t.Error("nodata on single band tiff not raised")
	}
	/* TODO:
	err = uds.Bands()[0].ClearNoData()
	if err == nil {
		t.Error("clear nodata on single band tiff not raised")
	}
	*/
	err = uds.SetNoData(5)
	if err != nil {
		t.Error(err)
	}
	_, err = uds.CreateMaskBand(0x2)
	if err != nil {
		t.Error(err)
	}
	err = uds.Close()
	if err != nil {
		t.Error(err)
	}
	uds, _ = Open(tt, SiblingFiles(filepath.Base(tt)))
	flags := uds.Bands()[0].MaskFlags()
	if flags != 0x8 {
		t.Errorf("mask was used: %d", flags)
	}
	_ = uds.Close()
	uds, _ = Open(tt, SiblingFiles(filepath.Base(tt), filepath.Base(tt+".msk")))
	flags = uds.Bands()[0].MaskFlags()
	if flags != 0x2 {
		t.Errorf("mask was not used: %d", flags)
	}
	nd, _ := uds.Bands()[0].NoData()
	if nd != 5 {
		t.Errorf("nodata=%f", nd)
	}
	_ = uds.Close()
}

func TestClosingErrors(t *testing.T) {
	//hacky test to force Dataset.Close() to return an error. we use the fact that
	//the geojson drivers uses a temp file when updating an exisiting dataset

	tmpdir, _ := ioutil.TempDir("", "")
	fname := filepath.Join(tmpdir, "tt.json")
	defer func() {
		_ = os.Chmod(fname, 0777)
		_ = os.Chmod(tmpdir, 0777)
		_ = os.RemoveAll(tmpdir)
	}()
	sds, err := Open("testdata/test.geojson")
	assert.NoError(t, err)
	rds, err := sds.VectorTranslate(fname, []string{"-f", "GeoJSON"})
	assert.NoError(t, err)
	_ = sds.Close()
	_ = rds.Close()
	rds, _ = Open(fname, Update())
	_ = os.Chmod(fname, 0400)
	_ = os.Chmod(tmpdir, 0400)
	_ = rds.SetMetadata("foo", "bar")
	rds.Layers()[0].ResetReading()
	f := rds.Layers()[0].NextFeature()
	ng, err := f.Geometry().Buffer(1, 1)
	assert.NoError(t, err)
	_ = f.SetGeometry(ng)
	_ = rds.Layers()[0].UpdateFeature(f)
	err = rds.Close()
	assert.Error(t, err)
}

func TestOpenShared(t *testing.T) {
	ds, _ := Open("testdata/test.tif", Shared())
	vds, _ := ds.Translate("", []string{"-of", "VRT"})
	_ = ds.Close()
	data := make([]uint8, 100)
	err := vds.Read(0, 0, data, 10, 10) //this will segfault if ds is not opened with Shared()
	assert.NoError(t, err)
	_ = vds.Close()
}

func TestRegister(t *testing.T) {
	err := RegisterRaster("GTiff")
	assert.NoError(t, err)
	err = RegisterRaster(GTiff)
	assert.NoError(t, err)
	err = RegisterVector("GTiff")
	assert.Error(t, err)
	err = RegisterVector(GTiff)
	assert.Error(t, err)

	err = RegisterRaster("bogus")
	assert.Error(t, err)
	err = RegisterVector("bogus")
	assert.Error(t, err)

	err = RegisterVector("VRT")
	assert.NoError(t, err)
	err = RegisterVector(VRT)
	assert.NoError(t, err)
	err = RegisterRaster("VRT")
	assert.NoError(t, err)
	err = RegisterRaster(VRT)
	assert.NoError(t, err)

	err = RegisterRaster("GeoJSON")
	assert.Error(t, err)
	err = RegisterRaster(GeoJSON)
	assert.Error(t, err)
}
