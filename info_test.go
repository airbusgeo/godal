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
	"github.com/stretchr/testify/require"
)

func TestSize(t *testing.T) {
	ds, err := Open("testdata/test.tif")
	srm, _ := NewSpatialRefFromEPSG(3857)
	require.NoError(t, err)
	st := ds.Structure()
	assert.Equal(t, 10, st.SizeX)
	assert.Equal(t, 10, st.SizeY)

	bounds, err := ds.Bounds()
	assert.NoError(t, err)
	assert.Equal(t, [4]float64{45, 25, 55, 35}, bounds)
	bounds, err = ds.Bounds(srm)
	assert.NoError(t, err)
	assert.NotEqual(t, [4]float64{45, 25, 55, 35}, bounds)
	_, err = ds.Bounds(&SpatialRef{})
	assert.Error(t, err)

	mds, _ := ds.Translate("", []string{"-of", "MEM"})
	gt, _ := ds.GeoTransform()
	ds.Close()
	gt[5] = -gt[5]
	gt[1] = -gt[1]
	err = mds.SetGeoTransform(gt)
	assert.NoError(t, err)
	bounds, err = mds.Bounds()
	assert.NoError(t, err)
	assert.Equal(t, [4]float64{35, 35, 45, 45}, bounds)
	bounds, err = mds.Bounds(srm)
	assert.NoError(t, err)
	assert.NotEqual(t, [4]float64{35, 35, 45, 45}, bounds)
	mds.Close()

}

func TestBands(t *testing.T) {
	ds, err := Open("testdata/test.tif")
	require.NoError(t, err)
	bands := ds.Bands()
	assert.Len(t, bands, 3)
	bst := bands[0].Structure()
	assert.Equal(t, 256, bst.BlockSizeX)
	assert.Equal(t, 256, bst.BlockSizeY)
	dt := bands[1].Structure().DataType
	assert.Equal(t, Byte, dt)
	assert.Equal(t, "Byte", dt.String())
	nd, ok := bands[2].NoData()
	assert.Equal(t, true, ok)
	assert.Equal(t, 99.0, nd)
}

func TestNoData(t *testing.T) {
	ds, err := Create(Memory, "ffff", 2, Byte, 20, 20)
	require.NoError(t, err)
	defer ds.Close()
	bands := ds.Bands()
	err = bands[1].SetNoData(100)
	assert.NoError(t, err)
	nd, ok := bands[1].NoData()
	assert.Equal(t, 100.0, nd)
	assert.Equal(t, true, ok)
	err = bands[1].ClearNoData()
	assert.NoError(t, err)
	_, ok = bands[1].NoData()
	assert.Equal(t, false, ok)
	err = ds.SetNoData(101)
	assert.NoError(t, err)
	nd, ok = bands[0].NoData()
	assert.Equal(t, 101.0, nd)
	assert.Equal(t, true, ok)
}

func TestStructure(t *testing.T) {
	tmpname := tempfile()
	defer os.Remove(tmpname)
	ds, err := Create(GTiff, tmpname, 3, Byte, 64, 64, CreationOption("TILED=YES", "BLOCKXSIZE=32", "BLOCKYSIZE=32"))
	require.NoError(t, err)
	st := ds.Structure()
	assert.Equal(t, 64, st.SizeX)
	assert.Equal(t, 64, st.SizeY)
	assert.Equal(t, 32, st.BlockSizeX)
	assert.Equal(t, 32, st.BlockSizeY)
	if x, y := st.BlockCount(); x != 2 || y != 2 {
		t.Errorf("cx,cy: %d,%d", x, y)
	}
	if x, y := st.ActualBlockSize(0, 0); x != 32 || y != 32 {
		t.Errorf("abx,abyy: %d,%d", x, y)
	}
	if x, y := st.ActualBlockSize(1, 1); x != 32 || y != 32 {
		t.Errorf("abx,abyy: %d,%d", x, y)
	}
	if x, y := st.ActualBlockSize(2, 2); x != 0 || y != 0 {
		t.Errorf("abx,abyy: %d,%d", x, y)
	}
	ds.Close()

	ds, err = Create(GTiff, tmpname, 3, Byte, 65, 65, CreationOption("TILED=YES", "BLOCKXSIZE=32", "BLOCKYSIZE=32"))
	require.NoError(t, err)
	st = ds.Structure()
	if x, y := st.BlockCount(); x != 3 || y != 3 {
		t.Errorf("cx,cy: %d,%d", x, y)
	}
	if x, y := st.ActualBlockSize(2, 2); x != 1 || y != 1 {
		t.Errorf("abx,abyy: %d,%d", x, y)
	}
	if x, y := st.ActualBlockSize(1, 1); x != 32 || y != 32 {
		t.Errorf("abx,abyy: %d,%d", x, y)
	}
	if x, y := st.ActualBlockSize(3, 3); x != 0 || y != 0 {
		t.Errorf("abx,abyy: %d,%d", x, y)
	}
	ds.Close()

	ds, err = Create(GTiff, tmpname, 3, Byte, 63, 63, CreationOption("TILED=YES", "BLOCKXSIZE=32", "BLOCKYSIZE=32"))
	require.NoError(t, err)
	st = ds.Structure()
	if x, y := st.BlockCount(); x != 2 || y != 2 {
		t.Errorf("cx,cy: %d,%d", x, y)
	}
	if x, y := st.ActualBlockSize(2, 2); x != 0 || y != 0 {
		t.Errorf("abx,abyy: %d,%d", x, y)
	}
	if x, y := st.ActualBlockSize(1, 1); x != 31 || y != 31 {
		t.Errorf("abx,abyy: %d,%d", x, y)
	}
	if x, y := st.ActualBlockSize(0, 0); x != 32 || y != 32 {
		t.Errorf("abx,abyy: %d,%d", x, y)
	}
	ds.Close()
}

func TestVersion(t *testing.T) {
	AssertMinVersion(3, 2, 0)
	assert.Panics(t, func() { AssertMinVersion(99, 99, 99) })
}
