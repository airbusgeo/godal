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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatasetMask(t *testing.T) {
	tmpname := tempfile()
	defer os.Remove(tmpname)
	ds, err := Create(GTiff, tmpname, 1, Byte, 20, 20)
	if err != nil {
		t.Fatal(err)
	}
	bnd := ds.Bands()[0]
	mflag := bnd.MaskFlags()
	if mflag != 0x01 {
		t.Errorf("mflag: %d", mflag)
	}
	_, err = ds.CreateMaskBand(0x02, ConfigOption("GDAL_TIFF_INTERNAL_MASK=YES"))
	if err != nil {
		t.Fatal(err)
	}
	mflag = bnd.MaskFlags()
	if mflag != 0x02 {
		t.Errorf("flag: %d", mflag)
	}
	ds.Close()
	_, err = os.Stat(tmpname + ".msk")
	if err == nil {
		t.Error(".msk was created")
	}
}
func TestBandMask(t *testing.T) {
	tmpname := tempfile()
	defer os.Remove(tmpname)
	defer os.Remove(tmpname + ".msk")
	ds, err := Create(GTiff, tmpname, 1, Byte, 20, 20)
	if err != nil {
		t.Fatal(err)
	}
	bnd := ds.Bands()[0]
	mflag := bnd.MaskFlags()
	if mflag != 0x01 {
		t.Errorf("mflag: %d", mflag)
	}
	_, err = bnd.CreateMask(0x00, ConfigOption("GDAL_TIFF_INTERNAL_MASK=NO"))
	if err != nil {
		t.Fatal(err)
	}
	mflag = bnd.MaskFlags()
	if mflag != 0x00 {
		t.Errorf("flag: %d", mflag)
	}
	ds.Close()
	_, err = os.Stat(tmpname + ".msk")
	if err != nil {
		t.Errorf(".msk was not created: %v", err)
	}
}

func TestSetNoData(t *testing.T) {
	_ = RegisterRaster("HFA")
	ds, _ := Open("testdata/test.img")
	err := ds.SetNoData(0.5)
	if err == nil {
		t.Error("err not raised")
	}
	err = ds.Bands()[0].SetNoData(0.8)
	if err == nil {
		t.Error("err not raised")
	}
	bndnil := ds.Bands()[0]
	bndnil.handle = nil
	err = bndnil.ClearNoData()
	assert.Error(t, err)
}
