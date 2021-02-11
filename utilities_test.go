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
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTranslate(t *testing.T) {
	tmpname := tempfile()
	tmpname2 := tempfile()
	defer os.Remove(tmpname)
	defer os.Remove(tmpname2)

	ds, err := Create(GTiff, tmpname, 1, Byte, 20, 20)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	_, err = ds.Translate(tmpname2, []string{"-bogus"})
	if err == nil {
		t.Error("invalid switch not detected")
	}
	_, err = ds.Translate(tmpname2, nil, CreationOption("BAR=BAZ"))
	if err == nil {
		t.Error("invalid creation option not detected")
	}
	ds2, err := ds.Translate(tmpname2, []string{"-outsize", "200%", "200%"}, CreationOption("TILED=YES", "BLOCKXSIZE=32", "BLOCKYSIZE=16"), GTiff)
	if err != nil {
		t.Fatal(err)
	}
	defer ds2.Close()
	st := ds2.Structure()
	if st.SizeX != 40 || st.SizeY != 40 {
		t.Errorf("wrong size %d,%d", st.SizeX, st.SizeY)
	}
	if st.BlockSizeX != 32 || st.BlockSizeY != 16 {
		t.Errorf("wrong block size %d,%d", st.BlockSizeX, st.BlockSizeY)
	}
}
func TestDatasetWarp(t *testing.T) {
	tmpname := tempfile()
	tmpname2 := tempfile()
	defer os.Remove(tmpname)
	defer os.Remove(tmpname2)

	ds, err := Create(GTiff, tmpname, 1, Byte, 20, 20)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	sr, _ := NewSpatialRefFromEPSG(3857)
	err = ds.SetSpatialRef(sr)
	if err != nil {
		t.Error(err)
	}
	err = ds.SetGeoTransform([6]float64{0, 2, 0, 0, 0, -2})
	if err != nil {
		t.Error(err)
	}
	_, err = ds.Warp(tmpname2, []string{"-bogus"})
	if err == nil {
		t.Error("invalid switch not detected")
	}
	/* TODO
	ds2, err = ds.Warp(tmpname2, nil, CreationOption("BAR=BAZ"))
	if err == nil {
		t.Error("invalid creation option not detected")
	}
	*/
	ds2, err := ds.Warp(tmpname2, []string{"-ts", "40", "40"}, CreationOption("TILED=YES", "BLOCKXSIZE=32", "BLOCKYSIZE=16"), GTiff)
	if err != nil {
		t.Fatal(err)
	}
	defer ds2.Close()
	st := ds2.Structure()
	if st.SizeX != 40 || st.SizeY != 40 {
		t.Errorf("wrong size %d,%d", st.SizeX, st.SizeY)
	}
	if st.BlockSizeX != 32 || st.BlockSizeY != 16 {
		t.Errorf("wrong block size %d,%d", st.BlockSizeX, st.BlockSizeY)
	}
}
func TestDatasetWarpMulti(t *testing.T) {
	ds1, _ := Create(Memory, "", 3, Byte, 5, 5)
	ds2, _ := Create(Memory, "", 3, Byte, 5, 5)

	sr, _ := NewSpatialRefFromEPSG(4326)
	_ = ds1.SetSpatialRef(sr)
	_ = ds2.SetSpatialRef(sr)

	_ = ds1.SetGeoTransform([6]float64{45, 1, 0, 35, 0, -1})
	_ = ds2.SetGeoTransform([6]float64{50, 1, 0, 35, 0, -1})

	for _, b := range ds1.Bands() {
		_ = b.Fill(200, 0)
	}

	for _, b := range ds2.Bands() {
		_ = b.Fill(100, 0)
	}

	defer ds1.Close()
	defer ds2.Close()

	// Warp NewDataset with multiple input dataset
	filePath := path.Join(os.TempDir(), "warp.tif")
	outputDataset, err := Warp(filePath, []*Dataset{ds1, ds2}, []string{}, CreationOption("TILED=YES"), GTiff)
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(filePath)
	defer outputDataset.Close()

	data := make([]uint8, 50)
	err = outputDataset.Read(0, 0, data, outputDataset.Structure().SizeX, outputDataset.Structure().SizeY, Bands(0, 1, 2), Window(10, 10))
	assert.Error(t, err, "Access window out of range")

	// read total warp result
	err = outputDataset.Read(0, 0, data, outputDataset.Structure().SizeX, outputDataset.Structure().SizeY,
		Bands(0, 1, 2),
		Window(outputDataset.Structure().SizeX, outputDataset.Structure().SizeY),
	)
	assert.NoError(t, err)

	assert.Equal(t, []uint8{200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 100, 100, 100, 100, 100}, data)

	// read part warp result
	err = outputDataset.Read(0, 0, data, ds1.Structure().SizeX, ds1.Structure().SizeY,Bands(0, 1, 2))
	assert.NoError(t, err)

	for _, px := range data {
		if px != 200 {
			t.Errorf("wrong pixel value : %d instead of 200", px)
		}
	}

	err = outputDataset.Read(5, 0, data, ds2.Structure().SizeX, ds2.Structure().SizeY,Bands(0, 1, 2))
	assert.NoError(t, err)

	for _, px := range data {
		if px != 100 {
			t.Errorf("wrong pixel value : %d instead of 100", px)
		}
	}
}
func TestDatasetWarpInto(t *testing.T) {
	outputDataset, _ := Create(Memory, "", 1, Byte, 5, 5)
	inputDataset, _ := Create(Memory, "", 1, Byte, 5, 5)

	for _, b := range outputDataset.Bands() {
		_ = b.Fill(200, 0)
	}

	for _, b := range inputDataset.Bands() {
		_ = b.Fill(155, 0)
	}

	sr, _ := NewSpatialRefFromEPSG(4326)
	_ = outputDataset.SetSpatialRef(sr)
	_ = outputDataset.SetGeoTransform([6]float64{45, 1, 0, 35, 0, -1})
	_ = inputDataset.SetSpatialRef(sr)
	_ = inputDataset.SetGeoTransform([6]float64{45, 1, 0, 35, 0, -1})

	defer outputDataset.Close()
	defer inputDataset.Close()

	// Warp existing dataset with multiple input dataset
	err := outputDataset.WarpInto([]*Dataset{inputDataset}, []string{"-co", "TILED=YES"})
	assert.Error(t, err, "All options related to creation ignored in update mode")

	if err = outputDataset.WarpInto([]*Dataset{inputDataset}, []string{}, ConfigOption("GDAL_CACHEMAX=64")); err != nil {
		t.Fatal(err)
	}

	if outputDataset.Structure().NBands != 1 {
		t.Errorf("wrong band number : %d", outputDataset.Structure().NBands)
	}

	data := make([]uint8, 25)
	err = outputDataset.Read(0, 0, data, outputDataset.Structure().SizeX, outputDataset.Structure().SizeY, Bands(0))
	assert.NoError(t, err)

	for _, px := range data {
		if px != 155 {
			t.Errorf("wrong px value : %d instead of 155", px)
		}
	}

}
func TestBuildOverviews(t *testing.T) {
	tmpname := tempfile()
	defer os.Remove(tmpname)
	ds, err := Create(GTiff, tmpname, 1, Byte, 20, 20, CreationOption("TILED=YES", "BLOCKXSIZE=128", "BLOCKYSIZE=128"))
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	err = ds.BuildOverviews()
	if err != nil {
		t.Error(err)
	}
	if len(ds.Bands()[0].Overviews()) != 0 {
		t.Errorf("expected 0 overviews")
	}

	tmpname = tempfile()
	defer os.Remove(tmpname)

	ds, err = Create(GTiff, tmpname, 2, Byte, 2000, 2000, CreationOption("TILED=YES", "BLOCKXSIZE=256", "BLOCKYSIZE=256"))
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	err = ds.BuildOverviews()
	if err != nil {
		t.Error(err)
	}
	if len(ds.Bands()[0].Overviews()) != 3 {
		t.Errorf("expected 3 overviews")
	}
	err = ds.ClearOverviews()
	if err != nil {
		t.Error(err)
	}
	err = ds.BuildOverviews(MinSize(200))
	if err != nil {
		t.Error(err)
	}
	ovrs := ds.Bands()[0].Overviews()
	l200 := false
	for _, ovr := range ovrs {
		st := ovr.Structure()
		if st.SizeX <= 200 || st.SizeY <= 200 {
			l200 = true
		}
		if st.SizeX <= 100 && st.SizeY <= 100 {
			t.Errorf("have overview of size %dx%d", st.SizeX, st.SizeY)
		}
	}
	if !l200 {
		t.Error("missing overview <200")
	}
	err = ds.ClearOverviews()
	if err != nil {
		t.Error(err)
	}
	for i, bnd := range ds.Bands() {
		if bnd.Overviews() != nil {
			t.Errorf("band %d has overviews", i)
		}
	}
	err = ds.BuildOverviews(Levels(2, 4))
	if err != nil {
		t.Error(err)
	}
	if len(ds.Bands()[0].Overviews()) != 2 {
		t.Errorf("expected 2 overviews")
	}
	_ = ds.ClearOverviews()
	err = ds.BuildOverviews(Levels(1, 2))
	if err == nil {
		t.Error("invalid overview level not raised")
	}
	if len(ds.Bands()[0].Overviews()) != 0 {
		t.Errorf("expected 0 overviews")
	}

	_ = ds.ClearOverviews()
	err = ds.BuildOverviews(Bands(1), Levels(2, 4))
	if err == nil {
		t.Error("unsupported building of overviews on single band not raised")
	}
	if len(ds.Bands()[0].Overviews()) != 0 {
		t.Errorf("band 0 expected 0 overviews")
	}
	if len(ds.Bands()[1].Overviews()) != 0 {
		t.Errorf("band 0 expected 0 overviews")
	}

	/* TODO find a driver that supports building overviews for a single band. disabled for now
	ds, _ = Create(Memory,"", 2, Byte, 2000, 2000)
	defer ds.Close()
	err = ds.BuildOverviews(Bands(1), Levels(2, 4))
	if err != nil {
		t.Error(err)
	}
	if ds.Bands()[0].OverviewCount() != 0 {
		t.Errorf("band 0 expected 0 overviews")
	}
	if ds.Bands()[1].OverviewCount() != 2 {
		t.Errorf("band 1 expected 2 overviews")
	}
	*/
}

func TestResampling(t *testing.T) {
	ds, _ := Create(Memory, "", 1, Byte, 10, 10)
	data := make([]uint8, 100)
	for i := range data {
		data[i] = byte(i)
	}
	_ = ds.Write(0, 0, data, 10, 10)

	exp := map[ResamplingAlg][3]uint8{
		Nearest:     {3, 3, 11},
		Average:     {2, 2, 6},
		Cubic:       {3, 3, 12},
		Bilinear:    {3, 3, 14},
		CubicSpline: {3, 3, 18},
		Gauss:       {3, 3, 22},
		Lanczos:     {3, 3, 11},
		Mode:        {3, 3, 0},
		Max:         {3, 3, 255},
		Min:         {3, 3, 255},
		Median:      {3, 3, 255},
		Q1:          {3, 3, 255},
		Q3:          {3, 3, 255},
		Sum:         {3, 3, 255},
	}

	for a, v := range exp {
		err := ds.Read(0, 0, data, 1, 1, Window(int(v[0]), int(v[1])), Resampling(a))
		if v[2] == 255 {
			assert.Error(t, err, "%s io resampling error not raised", a.String())
		} else {
			assert.NoError(t, err)
			assert.EqualValues(t, v[2], data[0], "%s resampling error", a.String())
		}
		err = ds.Bands()[0].Read(0, 0, data, 1, 1, Window(int(v[0]), int(v[1])), Resampling(a))
		if v[2] == 255 {
			assert.Error(t, err, "%s io resampling error not raised", a.String())
		} else {
			assert.NoError(t, err)
			assert.EqualValues(t, v[2], data[0], "%s resampling error", a.String())
		}
	}

	assert.Panics(t, func() { _ = ds.Bands()[0].Read(0, 0, data, 1, 1, Resampling(100)) })

	ovrs := map[ResamplingAlg]uint8{
		Nearest:     0,
		Average:     6,
		Cubic:       6,
		Bilinear:    8,
		CubicSpline: 10,
		Gauss:       11,
		Lanczos:     6,
		Mode:        0,
		Max:         255,
		Min:         255,
		Median:      255,
		Q1:          255,
		Q3:          255,
		Sum:         255,
	}
	for a, v := range ovrs {
		_ = ds.ClearOverviews()
		if v != 255 {
			err := ds.BuildOverviews(Resampling(a), Levels(2))
			assert.NoError(t, err)
			err = ds.Bands()[0].Overviews()[0].Read(0, 0, data, 1, 1)
			assert.NoError(t, err)
			assert.EqualValues(t, v, data[0], "%s resampling error", a.String())
		} else {
			err := ds.BuildOverviews(Resampling(a), Levels(2))
			assert.Error(t, err, "%s overview resampling error not raised", a.String())
		}
	}
}

func TestPolygonize(t *testing.T) {
	rds, _ := Create(Memory, "", 2, Byte, 8, 8)
	vds, err := CreateVector(Memory, "")
	if err != nil {
		t.Fatal(err)
	}
	pl4, _ := vds.CreateLayer("p4", nil, GTPolygon)
	pl8, _ := vds.CreateLayer("p8", nil, GTPolygon)
	data := make([]byte, 64)
	for r := 0; r < 8; r++ {
		for c := 0; c < 8; c++ {
			if r == c {
				data[r*8+c] = 128
			} else {
				data[r*8+c] = 64
			}
		}
	}
	bnd := rds.Bands()[0]
	_ = bnd.Write(0, 0, data, 8, 8)
	err = bnd.Polygonize(pl4, PixelValueFieldIndex(5))
	if err == nil {
		t.Error("invalid field not raised")
	}
	err = bnd.Polygonize(pl4)
	if err != nil {
		t.Error(err)
	}
	cnt, _ := pl4.FeatureCount()
	if cnt != 10 {
		t.Errorf("got %d/10 polys", cnt)
	}
	err = bnd.Polygonize(pl8, EightConnected())
	if err != nil {
		t.Error(err)
	}
	cnt, _ = pl8.FeatureCount()
	if cnt != 2 {
		t.Errorf("got %d/2 polys", cnt)
	}

	msk, err := bnd.CreateMask(0x02)
	if err != nil {
		t.Fatal(err)
	}
	for r := 0; r < 8; r++ {
		for c := 0; c < 8; c++ {
			if r == c {
				data[r*8+c] = 0
			} else {
				data[r*8+c] = 255
			}
		}
	}
	_ = msk.Write(0, 0, data, 8, 8)

	nd, _ := vds.CreateLayer("nd", nil, GTPolygon, NewFieldDefinition("unused", FTString), NewFieldDefinition("c", FTInt))
	err = bnd.Polygonize(nd, PixelValueFieldIndex(1))
	if err != nil {
		t.Error(err)
	}
	cnt, _ = nd.FeatureCount()
	if cnt != 2 {
		t.Errorf("got %d/2 polys", cnt)
	}
	attrs := nd.NextFeature().Fields()
	if attrs["c"].Int() != 64 && attrs["c"].Int() != 128 {
		t.Error("expecting 64 or 128 for pixel attribute")
	}
	nm, _ := vds.CreateLayer("nm", nil, GTPolygon)
	err = bnd.Polygonize(nm, NoMask())
	if err != nil {
		t.Error(err)
	}
	cnt, _ = nm.FeatureCount()
	if cnt != 10 {
		t.Errorf("got %d/10 polys", cnt)
	}

	//one quarter is nodata
	for r := 0; r < 8; r++ {
		for c := 0; c < 8; c++ {
			if r < 4 && c < 4 {
				data[r*8+c] = 0
			} else {
				data[r*8+c] = 255
			}
		}
	}
	_ = rds.Bands()[1].Write(0, 0, data, 8, 8)
	for r := 0; r < 8; r++ {
		for c := 0; c < 8; c++ {
			data[r*8+c] = uint8(r*8 + c)
		}
	}
	_ = bnd.Write(0, 0, data, 8, 8)

	md, _ := vds.CreateLayer("md", nil, GTPolygon)
	err = bnd.Polygonize(md, Mask(rds.Bands()[1]))
	if err != nil {
		t.Error(err)
	}
	cnt, _ = md.FeatureCount()
	if cnt != 48 { // 48 == 64 - 64/4
		t.Errorf("got %d/48 polys", cnt)
	}
}

func TestRasterize(t *testing.T) {
	tf := tempfile()
	defer os.Remove(tf)
	inv, _ := Open("testdata/test.geojson", VectorOnly())

	_, err := inv.Rasterize(tf, []string{"-of", "bogus"})
	if err == nil {
		t.Error("error not raised")
	}
	rds, err := inv.Rasterize(tf, []string{
		"-te", "99", "-1", "102", "2",
		"-ts", "9", "9",
		"-init", "10",
		"-burn", "20"}, CreationOption("TILED=YES"), GTiff)
	if err != nil {
		t.Fatal(err)
	}
	defer rds.Close()
	data := make([]byte, 81)
	err = rds.Read(0, 0, data, 9, 9)
	if err != nil {
		t.Fatal(err)
	}
	n10 := 0
	n20 := 0
	for i := range data {
		if data[i] == 10 {
			n10++
		}
		if data[i] == 20 {
			n20++
		}
	}
	if n10 != 72 || n20 != 9 {
		t.Errorf("10/20: %d/%d expected 72/9", n10, n20) //not really tested here, although sum should always be 81
	}

}

func TestRasterizeGeometries(t *testing.T) {
	vds, _ := Open("testdata/test.geojson")
	//ext is 100,0,101,1
	defer vds.Close()
	mds, _ := Create(Memory, "", 3, Byte, 300, 300)
	defer mds.Close()
	_ = mds.SetGeoTransform([6]float64{99, 0.01, 0, 2, 0, -0.01}) //set extent to 99,-1,102,2
	bnds := mds.Bands()

	ff := vds.Layers()[0].NextFeature().Geometry()

	for _, bnd := range bnds {
		_ = bnd.Fill(255, 0)
	}
	data := make([]byte, 300) //to extract a 10x10 window

	err := mds.RasterizeGeometry(ff)
	assert.NoError(t, err)
	_ = mds.Read(95, 95, data, 10, 10)
	assert.Equal(t, []byte{255, 255, 255}, data[0:3])
	assert.Equal(t, []byte{0, 0, 0}, data[297:300])

	alldata1 := make([]byte, 300*300*3)
	_ = mds.Read(0, 0, alldata1, 300, 300)
	alldata2 := make([]byte, 300*300*3)
	err = mds.RasterizeGeometry(ff, AllTouched())
	assert.NoError(t, err)
	_ = mds.Read(0, 0, alldata2, 300, 300)
	assert.NotEqual(t, alldata1, alldata2)

	err = mds.RasterizeGeometry(ff, Values(200))
	assert.NoError(t, err)
	_ = mds.Read(95, 95, data, 10, 10)
	assert.Equal(t, []byte{200, 200, 200}, data[297:300])

	err = mds.RasterizeGeometry(ff, Bands(0), Values(100))
	assert.NoError(t, err)
	_ = mds.Read(95, 95, data, 10, 10)
	assert.Equal(t, []byte{100, 200, 200}, data[297:300])

	err = mds.RasterizeGeometry(ff, Values(1, 2, 3))
	assert.NoError(t, err)
	_ = mds.Read(95, 95, data, 10, 10)
	assert.Equal(t, []uint8{1, 2, 3}, data[297:300])

	err = mds.RasterizeGeometry(ff, Bands(0, 1), Values(5, 6))
	assert.NoError(t, err)
	_ = mds.Read(95, 95, data, 10, 10)
	assert.Equal(t, []uint8{5, 6, 3}, data[297:300])

	err = mds.RasterizeGeometry(ff, Bands(0), Values(1, 2))
	assert.Error(t, err)
	err = mds.RasterizeGeometry(ff, Bands(0, 2, 3), Values(1, 2, 3))
	assert.Error(t, err)

}
