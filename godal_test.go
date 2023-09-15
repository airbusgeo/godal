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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/airbusgeo/osio"
	"github.com/airbusgeo/osio/gcs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func init() {
	RegisterInternalDrivers()
}

type errChecker struct {
	errs int
}

func (e *errChecker) ErrorHandler(ec ErrorCategory, code int, message string) error {

	if ec >= CE_Warning {
		e.errs++
		return errors.New(message)
	}
	return nil
}

func eh() *errChecker {
	return &errChecker{}
}

func tempfile() string {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	}
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

func TestCBuffer(t *testing.T) {
	var buf interface{}
	buf = make([]byte, 100)
	_ = cBuffer(buf, 100)
	assert.Equal(t, Byte, bufferType(buf))
	assert.Equal(t, 1, bufferType(buf).Size())
	assert.Panics(t, func() { cBuffer(buf, 101) })

	buf = make([]int16, 100)
	_ = cBuffer(buf, 100)
	assert.Equal(t, Int16, bufferType(buf))
	assert.Equal(t, 2, bufferType(buf).Size())
	assert.Panics(t, func() { cBuffer(buf, 101) })

	buf = make([]uint16, 100)
	_ = cBuffer(buf, 100)
	assert.Equal(t, UInt16, bufferType(buf))
	assert.Equal(t, 2, bufferType(buf).Size())
	assert.Panics(t, func() { cBuffer(buf, 101) })

	buf = make([]int32, 100)
	_ = cBuffer(buf, 100)
	assert.Equal(t, Int32, bufferType(buf))
	assert.Equal(t, 4, bufferType(buf).Size())
	assert.Panics(t, func() { cBuffer(buf, 101) })

	buf = make([]uint32, 100)
	_ = cBuffer(buf, 100)
	assert.Equal(t, UInt32, bufferType(buf))
	assert.Equal(t, 4, bufferType(buf).Size())
	assert.Panics(t, func() { cBuffer(buf, 101) })

	buf = make([]float32, 100)
	_ = cBuffer(buf, 100)
	assert.Equal(t, Float32, bufferType(buf))
	assert.Equal(t, 4, bufferType(buf).Size())
	assert.Panics(t, func() { cBuffer(buf, 101) })

	buf = make([]float64, 100)
	_ = cBuffer(buf, 100)
	assert.Equal(t, Float64, bufferType(buf))
	assert.Equal(t, 8, bufferType(buf).Size())
	assert.Panics(t, func() { cBuffer(buf, 101) })

	buf = make([]complex64, 100)
	_ = cBuffer(buf, 100)
	assert.Equal(t, CFloat32, bufferType(buf))
	assert.Equal(t, 8, bufferType(buf).Size())
	assert.Panics(t, func() { cBuffer(buf, 101) })

	buf = make([]complex128, 100)
	_ = cBuffer(buf, 100)
	assert.Equal(t, CFloat64, bufferType(buf))
	assert.Equal(t, 16, bufferType(buf).Size())
	assert.Panics(t, func() { cBuffer(buf, 101) })

	assert.Panics(t, func() { cBuffer("stringtest", 100) })
	assert.Panics(t, func() { bufferType("stringtest") })
}

func TestColorTable(t *testing.T) {
	ds, _ := Create(Memory, "", 1, Byte, 10, 10)
	defer ds.Close()
	bnd := ds.Bands()[0]
	ct := bnd.ColorTable()
	assert.Len(t, ct.Entries, 0)

	ct.PaletteInterp = CMYKPalette
	ct.Entries = [][4]int16{
		{1, 1, 1, 1},
		{2, 2, 2, 2},
	}
	err := bnd.SetColorTable(ct)
	assert.NoError(t, err)
	ehc := eh()
	err = bnd.SetColorTable(ct, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	assert.Equal(t, 0, ehc.errs)

	assert.Equal(t, CIPalette, bnd.ColorInterp())
	ct2 := bnd.ColorTable()
	assert.Equal(t, CMYKPalette, ct2.PaletteInterp)
	assert.Equal(t, ct.Entries, ct2.Entries)

	//clear
	ct2.Entries = nil
	err = bnd.SetColorTable(ct2)
	assert.NoError(t, err)
	ct3 := bnd.ColorTable()
	assert.Len(t, ct3.Entries, 0)
}

func TestCreate(t *testing.T) {
	tmpname := tempfile()
	defer os.Remove(tmpname)

	_, err := Create(GTiff, tmpname, 1, Byte, 20, 20, CreationOption("INVALID_OPT=BAR"))
	assert.Error(t, err)

	ehc := eh()
	_, err = Create(GTiff, tmpname, 1, Byte, 20, 20, CreationOption("INVALID_OPT=BAR"), ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	_, err = Create(GTiff, "/this/path/does/not/exist", 1, Byte, 10, 10)
	assert.Error(t, err)

	ds, err := Create(GTiff, tmpname, 1, Byte, 20, 20)
	driver := ds.Driver()
	assert.Equal(t, "GeoTIFF", driver.LongName())
	assert.Equal(t, "GTiff", driver.ShortName())
	assert.NoError(t, err)
	ds.Close()

	ehc = eh()
	ds, err = Create(GTiff, tmpname, 1, Byte, 20, 20, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	st := ds.Structure()
	bnds := ds.Bands()
	if st.SizeX != 20 || st.SizeY != 20 || len(bnds) != 1 || st.NBands != 1 {
		t.Error("fail")
	}
	ci := bnds[0].ColorInterp()
	if ci != CIGray || ci.Name() != "Gray" {
		t.Error(ci.Name())
	}
	err = bnds[0].SetColorInterp(CIRed)
	assert.NoError(t, err)
	ehc = eh()
	err = bnds[0].SetColorInterp(CIRed, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	ci = bnds[0].ColorInterp()
	if ci != CIRed {
		t.Error(ci.Name())
	}
	err = ds.Close()
	assert.NoError(t, err)

	st1, _ := os.Stat(tmpname)
	tmpname2 := tempfile()
	defer os.Remove(tmpname2)
	ds, err = Create(GTiff, tmpname2, 1, Byte, 20, 20, CreationOption("TILED=YES", "BLOCKXSIZE=128", "BLOCKYSIZE=128"))
	assert.NoError(t, err)

	ehc = eh()
	err = ds.Close(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	st2, _ := os.Stat(tmpname2)

	if st1.Size() == st2.Size() {
		t.Errorf("sizes: %d/%d", st1.Size(), st2.Size())
	}
}

func TestRegisterDrivers(t *testing.T) {
	_, ok := RasterDriver(HFA)
	assert.False(t, ok)

	_, err := Open("testdata/test.img")
	assert.Error(t, err)

	_ = RegisterRaster(HFA)
	_, ok = RasterDriver(HFA)
	assert.True(t, ok)

	_, ok = VectorDriver(HFA)
	assert.False(t, ok)

	_, ok = VectorDriver(GeoJSON)
	assert.True(t, ok)

	_, ok = RasterDriver(GeoJSON)
	assert.False(t, ok)

	ds, err := Open("testdata/test.img")
	assert.NoError(t, err)
	ds.Close()

	_, ok = RasterDriver("bazbaz")
	assert.False(t, ok)

	_, ok = VectorDriver("bazbaz")
	assert.False(t, ok)

	//make sur we can access driver through their real name and predefined var
	err = RegisterVector("TAB")
	assert.NoError(t, err)
	err = RegisterVector(Mitab)
	assert.NoError(t, err)
	_, ok = VectorDriver(Mitab)
	assert.True(t, ok)
	_, ok = VectorDriver("Mapinfo File")
	assert.True(t, ok)
}

func TestVectorCreate(t *testing.T) {
	tf := tempfile()
	defer os.Remove(tf)

	//driver cannot create a raster dataset
	_, err := Create(GeoJSON, tf, 1, Byte, 512, 512)
	assert.Error(t, err)
	_, err = CreateVector(GTiff, "")
	assert.Error(t, err)
	ehc := eh()
	_, err = CreateVector(GTiff, "", ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	_, err = Create("foobar", tf, 1, Byte, 512, 512)
	assert.Error(t, err)
	_, err = CreateVector("foobar", "")
	assert.Error(t, err)

	tf = tempfile()
	defer os.Remove(tf)
	_, err = CreateVector(GeoJSON, "/this/path/does/not/exist")
	if err == nil {
		t.Error("error not caught")
	}
	_, err = CreateVector(GeoJSON, tf, CreationOption("FOO=BAR"))
	if err == nil {
		t.Error("error not raised")
	}

	tf = tempfile()
	defer os.Remove(tf)
	ds, err := CreateVector(GeoJSON, tf)
	driver := ds.Driver()
	assert.Equal(t, "GeoJSON", driver.LongName())
	assert.Equal(t, "GeoJSON", driver.ShortName())
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	st := ds.Structure()
	if st.DataType != Unknown || st.NBands > 0 {
		t.Errorf("created raster %v", st)
	}
}

func TestMisc(t *testing.T) {
	assert.Panics(t, func() { Unknown.Size() }, "unknown datatype has no size")
}

func TestConfigOptions(t *testing.T) {
	tmpdir, _ := ioutil.TempDir("", "")
	tiffile := filepath.Join(tmpdir, "testfile.tif")
	tiffile2 := filepath.Join(tmpdir, "testfile2.tif")
	tfwfile := filepath.Join(tmpdir, "testfile.tfw")
	tiffile2msk := filepath.Join(tmpdir, "testfile2.tif.msk")
	defer func() {
		_ = os.RemoveAll(tmpdir)
	}()

	//Create. withtout the configoption create() will fail
	ds, err := Create(GTiff, tiffile, 1, Byte, 1024, 1024, CreationOption("INVALID_OPTION=TRUE"), ConfigOption("GDAL_VALIDATE_CREATION_OPTIONS=FALSE"))
	assert.NoError(t, err)
	_, _ = ds.CreateMaskBand(0x02) //tmpdir/testfile.msk
	_ = ds.Close()

	//Open
	err = ioutil.WriteFile(tfwfile, []byte(`1
0
0
-1
0
0
`), 0666)
	assert.NoError(t, err)

	//worldfile sidecar is ignored
	ds, _ = Open(tiffile)
	_, err = ds.GeoTransform()
	assert.Error(t, err)

	/* deactivated test as it does not error as it should. gdal bug?
	//worldfile sidecar is ignored when passing a list of files that does not contain worldfile
	ds2, _ := Open(tiffile, SiblingFiles("testfile2.tfw"))
	_, err = ds2.GeoTransform()
	assert.Error(t, err)
	gt2, _ := ds2.GeoTransform()
	assert.Equal(t, [6]float64{}, gt2)
	*/
	//geotransform read from worldfile
	ds3, _ := Open(tiffile, SiblingFiles())
	gt3, _ := ds3.GeoTransform()
	assert.Equal(t, [6]float64{-0.5, 1, 0, 0.5, 0, -1}, gt3)
	dsm, _ := ds.Translate(tiffile2, nil, GTiff, ConfigOption("GDAL_TIFF_INTERNAL_MASK=YES"))
	assert.NoFileExists(t, tiffile2msk)

	/* TODO: ConfigOption for WarpInto
	err = dsm.WarpInto([]*Dataset{ds}, nil, ConfigOption("GDAL_NUM_THREADS=-2", "CPL_DEBUG=ON"))
	assert.Error(t, err)
	*/
	dsm.Close()
	_ = os.Remove(tiffile2)

	_, err = ds.Warp(tiffile2, nil, ConfigOption("GDAL_NUM_THREADS=-2"))
	assert.Error(t, err)
	ehc := eh()
	_, err = ds.Warp(tiffile2, nil, ConfigOption("GDAL_NUM_THREADS=-2"), ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	ds.Close()
	//no geotransform if worldfile is ignored
	//no mask
	ds, _ = Open(tiffile, ConfigOption("GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR"))
	_, err = ds.GeoTransform()
	assert.Error(t, err)
	assert.NotEqual(t, 0x02, ds.Bands()[0].MaskFlags())
}

func TestHistogram(t *testing.T) {
	ds, _ := Create(Memory, "", 1, Byte, 16, 16)
	defer ds.Close()
	buf := make([]byte, 256)
	for i := range buf {
		buf[i] = byte(i)
	}
	_ = ds.Write(0, 0, buf, 16, 16)
	bnd := ds.Bands()[0]

	_, err := bnd.Histogram()
	assert.NoError(t, err)
	ehc := eh()
	hist, err := bnd.Histogram(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	ll := hist.Len()
	assert.Equal(t, 256, ll)
	for i := 0; i < ll; i++ {
		b := hist.Bucket(i)
		assert.Equal(t, float64(i)-0.5, b.Min)
		assert.Equal(t, float64(i+1)-0.5, b.Max)
		assert.Equal(t, uint64(1), b.Count)
	}

	hist, err = bnd.Histogram(IncludeOutOfRange(), Intervals(64, 63.5, 191.5))
	assert.NoError(t, err)
	ll = hist.Len()
	assert.Equal(t, 64, ll)
	for i := 0; i < ll; i++ {
		b := hist.Bucket(i)
		assert.Equal(t, 63.5+float64(i*2), b.Min)
		assert.Equal(t, 63.5+float64(i*2+2), b.Max)
		if i == 0 || i == ll-1 {
			assert.Equal(t, uint64(66), b.Count) //66 is the 64 preceding + the 2 of the actual bucket
		} else {
			assert.Equal(t, uint64(2), b.Count)
		}
	}
	_, err = bnd.Histogram(Approximate(), Intervals(64, 64, 192))
	assert.NoError(t, err)

	//to make histogram choke for coverage
	ebnd := Band{}
	_, err = ebnd.Histogram()
	assert.Error(t, err)

	ehc = eh()
	_, err = ebnd.Histogram(ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

}

func TestSize(t *testing.T) {
	ds, _ := Open("testdata/test.tif")
	srm, err := NewSpatialRefFromEPSG(3857)
	require.NoError(t, err)
	srm.Close()
	ehc := eh()
	srm, err = NewSpatialRefFromEPSG(3857, ErrLogger(ehc.ErrorHandler))
	require.NoError(t, err)
	st := ds.Structure()
	assert.Equal(t, 10, st.SizeX)
	assert.Equal(t, 10, st.SizeY)

	bounds, err := ds.Bounds()
	assert.NoError(t, err)
	/*
		ehc = eh()
		bounds, err = ds.Bounds(ErrLogger(ehc.ErrorHandler))
		assert.NoError(t, err)
	*/
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
	assert.Equal(t, 1.0, bst.Scale)
	assert.Equal(t, 0.0, bst.Offset)
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
	ehc := eh()
	err = bands[1].SetNoData(100, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	nd, ok := bands[1].NoData()
	assert.Equal(t, 100.0, nd)
	assert.Equal(t, true, ok)
	err = bands[1].ClearNoData()
	assert.NoError(t, err)
	ehc = eh()
	err = bands[1].ClearNoData(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	_, ok = bands[1].NoData()
	assert.Equal(t, false, ok)
	err = ds.SetNoData(101)
	assert.NoError(t, err)
	nd, ok = bands[0].NoData()
	assert.Equal(t, 101.0, nd)
	assert.Equal(t, true, ok)
}

func TestSetScale(t *testing.T) {
	ds, err := Create(Memory, "ffff", 2, Byte, 20, 20)
	require.NoError(t, err)
	defer ds.Close()
	bands := ds.Bands()
	err = bands[1].SetScaleOffset(100, 100)
	assert.NoError(t, err)
	ehc := eh()
	err = bands[1].SetScaleOffset(100, 100, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	st := bands[1].Structure()
	assert.Equal(t, 100.0, st.Scale)
	assert.Equal(t, 100.0, st.Offset)
	err = bands[1].ClearScaleOffset()
	assert.NoError(t, err)
	ehc = eh()
	err = bands[1].ClearScaleOffset(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	st = bands[1].Structure()
	assert.Equal(t, 1.0, st.Scale)
	assert.Equal(t, 0.0, st.Offset)
	err = ds.SetScaleOffset(101, 101)
	assert.NoError(t, err)
	st = bands[0].Structure()
	assert.Equal(t, 101.0, st.Scale)
	assert.Equal(t, 101.0, st.Offset)
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
	AssertMinVersion(3, 0, 0)
	assert.Panics(t, func() { AssertMinVersion(99, 99, 99) })
}

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
	_, err = ds.CreateMaskBand(0x02, ConfigOption("GDAL_TIFF_INTERNAL_MASK=YES"))
	assert.Error(t, err)
	ehc := eh()
	_, err = ds.CreateMaskBand(0x02, ConfigOption("GDAL_TIFF_INTERNAL_MASK=YES"), ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	_, err = ds.Bands()[0].CreateMask(0x02, ConfigOption("GDAL_TIFF_INTERNAL_MASK=YES"))
	assert.Error(t, err)
	ehc = eh()
	_, err = ds.Bands()[0].CreateMask(0x02, ConfigOption("GDAL_TIFF_INTERNAL_MASK=YES"), ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	err = ds.Bands()[0].Fill(5, 5)
	assert.Error(t, err)
	ehc = eh()
	err = ds.Bands()[0].Fill(5, 5, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

}
func TestDatasetRead(t *testing.T) {

	ds, err := Create(Memory, "", 3, Byte, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	bnds := ds.Bands()
	for i := range bnds {
		err = bnds[i].Fill(float64(10*i), 0)
		assert.NoError(t, err)
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
	assert.NoError(t, err)
	ehc := eh()
	err = bnd.Write(0, 0, buf, 10, 10, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	err = bnd.Read(95, 95, buf, 10, 10)
	assert.Error(t, err)
	ehc = eh()
	err = bnd.Read(95, 95, buf, 10, 10, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	err = bnd.Read(100, 100, buf, 10, 10)
	assert.Error(t, err)

	err = bnd.Read(0, 0, buf, 10, 10)
	assert.NoError(t, err)

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
	ds, _ := Create(Memory, "", 3, Byte, 2, 2)
	defer func() {
		_ = ds.Close()
	}()
	padData := []int32{
		1, 2, 3, 4, 5, 6, 7, 8, 9,
		10, 11, 12, 13, 14, 15, 16, 17, 18,
	}
	reset := func() {
		for i := range padData {
			padData[i] = 0
		}
	}
	_ = ds.Write(0, 0, padData, 2, 2, LineStride(9))
	reset()

	_ = ds.Read(0, 0, padData, 2, 2, PixelStride(4))
	assert.Equal(t, []int32{
		1, 2, 3, 0, 4, 5, 6, 0,
		10, 11, 12, 0, 13, 14, 15, 0,
	}, padData[0:16])
	reset()

	_ = ds.Read(0, 0, padData, 2, 2, PixelStride(1), LineStride(2), BandStride(4))
	assert.Equal(t, []int32{
		1, 4, 10, 13, //b1
		2, 5, 11, 14, //b2
		3, 6, 12, 15, //b3
	}, padData[0:12])
	reset()

	assert.Panics(t, func() {
		_ = ds.Read(0, 0, padData[0:12], 2, 2, PixelStride(1), LineStride(2), BandStride(5))
	})
	assert.Panics(t, func() {
		_ = ds.Read(0, 0, padData[0:12], 2, 2, PixelStride(1), LineStride(3), BandStride(4))
	})
	assert.Panics(t, func() {
		_ = ds.Read(0, 0, padData[0:12], 2, 2, PixelStride(2), LineStride(2), BandStride(4))
	})

	padData = padData[0:8] //single band tests
	bnd := ds.Bands()[0]
	_ = bnd.Read(0, 0, padData, 2, 2, PixelStride(2))
	assert.Equal(t, []int32{
		1, 0, 4, 0,
		10, 0, 13, 0,
	}, padData)
	reset()

	_ = bnd.Read(0, 0, padData, 2, 2, PixelStride(2), LineStride(3))
	assert.Equal(t, []int32{
		1, 0, 4,
		10, 0, 13,
		0, 0, //overflow
	}, padData)
	reset()

	assert.Panics(t, func() {
		_ = bnd.Read(0, 0, padData[0:4], 2, 2, PixelStride(1), LineStride(3))
	})
	assert.Panics(t, func() {
		_ = bnd.Read(0, 0, padData[0:4], 2, 2, PixelStride(2), LineStride(2))
	})

}
func TestSpacedIO(t *testing.T) {
	ds, _ := Create(Memory, "", 3, Byte, 8, 8)
	defer func() {
		_ = ds.Close()
	}()
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

func TestMetadata(t *testing.T) {
	tmpfname := tempfile()
	defer os.Remove(tmpfname)
	ds, _ := Create(GTiff, tmpfname, 1, Byte, 10, 10)

	md1 := ds.Metadata("foo")
	if md1 != "" {
		t.Error(md1)
	}
	md1 = ds.Metadata("foo", Domain("bar"))
	if md1 != "" {
		t.Error(md1)
	}
	err := ds.SetMetadata("foo", "bar")
	assert.NoError(t, err)
	ehc := eh()
	err = ds.SetMetadata("foo", "bar", ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	err = ds.SetMetadata("foo2", "bar2", Domain("baz"))
	assert.NoError(t, err)

	err = ds.SetDescription("desc")
	assert.NoError(t, err)
	err = ds.SetDescription("desc", ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	desc := ds.Description()
	assert.Equal(t, "desc", desc)

	md1 = ds.Metadata("foo")
	if md1 != "bar" {
		t.Error(md1)
	}
	md1 = ds.Metadata("foo2", Domain("baz"))
	if md1 != "bar2" {
		t.Error(md1)
	}

	mds := ds.Metadatas()
	if len(mds) != 1 {
		t.Error("empty")
	}
	for k, v := range mds {
		if k != "foo" || v != "bar" {
			t.Errorf("%s = %s", k, v)
		}
	}
	mds = ds.Metadatas(Domain("baz"))
	if len(mds) != 1 {
		t.Error("empty")
	}
	for k, v := range mds {
		if k != "foo2" || v != "bar2" {
			t.Errorf("%s = %s", k, v)
		}
	}
	mds = ds.Metadatas(Domain("bogus"))
	if len(mds) != 0 {
		t.Error("not empty")
	}

	_ = ds.SetMetadata("empty", "", Domain("empty"))
	mds = ds.Metadatas(Domain("empty"))
	if len(mds) != 1 {
		t.Errorf("empty: %d", len(mds))
	}
	for k, v := range mds {
		if k != "empty" || v != "" {
			t.Errorf("%s = %s", k, v)
		}
	}

	domains := ds.MetadataDomains()
	assert.Contains(t, domains, "")
	assert.Contains(t, domains, "empty")
	assert.Contains(t, domains, "baz")

	err = ds.ClearMetadata(Domain("empty"))
	assert.NoError(t, err)
	mds = ds.Metadatas(Domain("empty"))
	assert.Len(t, mds, 0)
	ehc = eh()
	err = ds.ClearMetadata(Domain("empty"), ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	ds.Close()
	err = ds.SetMetadata("foo", "bar")
	assert.Error(t, err)

}

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
	bndnil.cHandle = nil
	err = bndnil.ClearNoData()
	assert.Error(t, err)
}

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
	uds, _ = Open(tt)
	flags := uds.Bands()[0].MaskFlags()
	if flags != 0x8 {
		t.Errorf("mask was used: %d", flags)
	}
	_ = uds.Close()
	uds, _ = Open(tt, SiblingFiles(filepath.Base(tt+".msk")))
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
	data := make([]uint8, 300)
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

func TestTransform(t *testing.T) {
	sr1, _ := NewSpatialRefFromEPSG(4326)
	sr2, _ := NewSpatialRefFromEPSG(3857)
	_, err := NewTransform(sr1, sr2)
	assert.NoError(t, err)
	ehc := eh()
	ct, err := NewTransform(sr1, sr2, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	x := []float64{0, 1}
	y := []float64{0, 1}
	err = ct.TransformEx(x, y, nil, nil)
	if err != nil {
		t.Error(err)
	}
	if x[0] != 0 || y[0] != 0 {
		t.Errorf("failed: %f %f", x[0], y[0])
	}
	if x[1] == 1 || y[1] == 1 {
		t.Errorf("failed: %f %f", x[1], y[1])
	}

	x = []float64{0, 1}
	y = []float64{0, 1}
	z := []float64{0, 1}
	//TODO: make a test that actually checks z changes
	err = ct.TransformEx(x, y, z, nil)
	if err != nil {
		t.Error(err)
	}
	if x[0] != 0 || y[0] != 0 || z[0] != 0 {
		t.Errorf("failed: %f %f %f", x[0], y[0], z[0])
	}
	if x[1] == 1 || y[1] == 1 || z[1] != 1 {
		t.Errorf("failed: %f %f %f", x[1], y[1], z[1])
	}
	x = []float64{0, 1}
	y = []float64{0, 91}
	oks := []bool{false, false}
	err = ct.TransformEx(x, y, nil, oks)
	if err == nil {
		t.Error("failed trn not caught")
	}
	if !oks[0] {
		t.Error("ok[0] should be true")
	}
	if oks[1] {
		t.Error("ok[1] should be false")
	}
	ct.Close()
	assert.NotPanics(t, ct.Close, "2nd close must not panic")

	_, err = NewTransform(sr1, &SpatialRef{handle: nil})
	if err == nil {
		t.Error("err not raised")
	}
}
func TestProjection(t *testing.T) {
	tmpname := tempfile()
	defer os.Remove(tmpname)
	ds, err := Create(GTiff, tmpname, 1, Byte, 20, 20)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	pjs := ds.Projection()
	if pjs != "" {
		t.Errorf("non empty projection: %s", pjs)
	}
	_, err = NewSpatialRefFromEPSG(41234567898)
	if err == nil {
		t.Error("invalid epsg code not raised")
	}
	sr, err := NewSpatialRefFromEPSG(4326)
	if err != nil {
		t.Error(err)
	}
	defer sr.Close()
	err = ds.SetSpatialRef(sr)
	if err != nil {
		t.Error(err)
	}
	epsg4326 := `GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NORTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]`

	_, err = ds.SpatialRef().WKT()
	assert.NoError(t, err)
	ehc := eh()
	pj, err := ds.SpatialRef().WKT(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	if pj != epsg4326 {
		t.Error(pj)
	}

	_, err = NewSpatialRef("+proj=lonlat")
	assert.NoError(t, err)
	_, err = NewSpatialRef("epsg:3857")
	assert.NoError(t, err)
	_, err = NewSpatialRef(epsg4326)
	assert.NoError(t, err)
	_, err = NewSpatialRef("this won't work")
	assert.Error(t, err)
	_, err = NewSpatialRef("this won't work", ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	_, err = NewSpatialRef("+proj=lonlat", ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	_, err = NewSpatialRefFromProj4("invalid string")
	assert.Error(t, err)
	ehc = eh()
	_, err = NewSpatialRefFromProj4("invalid string", ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	_, err = NewSpatialRefFromProj4("+proj=lonlat")
	assert.NoError(t, err)
	ehc = eh()
	sr, err = NewSpatialRefFromProj4("+proj=lonlat", ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	defer sr.Close()
	_ = ds.SetSpatialRef(sr)

	pj, _ = ds.SpatialRef().WKT()
	ll := `GEOGCS["unknown",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]`

	if pj != ll {
		t.Error(pj)
	}

	_, err = NewSpatialRefFromWKT("invalid string")
	if err == nil {
		t.Error("invalid wkt not raised")
	}

	_, err = NewSpatialRefFromWKT(epsg4326)
	assert.NoError(t, err)
	ehc = eh()
	sr, err = NewSpatialRefFromWKT(epsg4326, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	_ = ds.SetSpatialRef(sr)
	pj, _ = ds.SpatialRef().WKT()

	if pj != epsg4326 {
		t.Error(pj)
	}

	//test closing on unowned sr
	sr = ds.SpatialRef()
	sr.Close()
	assert.NotPanics(t, sr.Close, "2nd close must not panic")

	err = ds.SetProjection("invalid wkt")
	assert.Error(t, err)
	err = ds.SetProjection(epsg4326)
	assert.NoError(t, err)
	err = ds.SetProjection("epsg:32618")
	assert.NoError(t, err)
	err = ds.SetProjection("+proj=lonlat")
	assert.NoError(t, err)

	err = ds.SetProjection("")
	assert.NoError(t, err)

	pj = ds.Projection()
	if pj != "" {
		t.Errorf("proj not empty: %s", pj)
	}

	ehc = eh()
	err = ds.SetProjection("invalid wkt", ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	ehc = eh()
	err = ds.SetProjection(epsg4326, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	pj = ds.Projection()
	if pj != epsg4326 {
		t.Error(pj)
	}

	err = ds.SetSpatialRef(nil)
	assert.NoError(t, err)
	ehc = eh()
	err = ds.SetSpatialRef(nil, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	pj = ds.Projection()
	if pj != "" {
		t.Errorf("proj not empty: %s", pj)
	}

	//hack to make setspatialref return an error for coverage
	ds.Close()
	err = ds.SetSpatialRef(nil)
	assert.Error(t, err)
	ehc = eh()
	err = ds.SetSpatialRef(nil, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
}

func TestNilSpatialRef(t *testing.T) {
	ds, _ := Open("testdata/test.tif")
	_ = ds.SetSpatialRef(nil)
	epsg4326, _ := NewSpatialRefFromEPSG(4326)
	sr := ds.SpatialRef()
	assert.False(t, sr.IsSame(epsg4326))
	assert.False(t, epsg4326.IsSame(sr))
	assert.False(t, sr.Geographic())
	assert.False(t, sr.Projected())
	_, err := sr.SemiMajor()
	assert.NoError(t, err)
	_, err = sr.SemiMajor()
	assert.NoError(t, err)
	a := sr.AuthorityCode("")
	assert.Empty(t, a)
	a = sr.AuthorityName("")
	assert.Empty(t, a)
	err = sr.AutoIdentifyEPSG()
	assert.Error(t, err)
	_, err = sr.WKT()
	assert.Error(t, err)
}

func TestProjMisc(t *testing.T) {
	ds, _ := Open("testdata/test.tif")
	sr := ds.SpatialRef()
	epsg4326, _ := NewSpatialRefFromEPSG(4326)
	assert.True(t, epsg4326.IsSame(sr), "isSame failed")
	assert.True(t, epsg4326.Geographic(), "not geographic")
	assert.True(t, epsg4326.EPSGTreatsAsLatLong())
	sm, err := sr.SemiMajor()
	assert.NoError(t, err)
	assert.Equal(t, 6.378137e+06, sm)
	sm, err = sr.SemiMinor()
	assert.NoError(t, err)
	assert.Equal(t, 6.356752314245179e+06, sm)
	au := sr.AuthorityName("")
	assert.Equal(t, "EPSG", au)
	au = sr.AuthorityName("GEOGCS|UNIT")
	assert.Equal(t, "EPSG", au)
	au = sr.AuthorityName("FOOBAR")
	assert.Equal(t, "", au)
	au = sr.AuthorityCode("")
	assert.Equal(t, "4326", au)
	au = sr.AuthorityCode("GEOGCS|UNIT")
	assert.Equal(t, "9122", au)
	au = sr.AuthorityCode("FOOBAR")
	assert.Equal(t, "", au)
	attr, ok := sr.AttrValue("GEOGCS", 0)
	assert.True(t, ok)
	assert.Equal(t, "WGS 84", attr)
	attr, ok = sr.AttrValue("GEOGCS", 9999)
	assert.False(t, ok)

	err = sr.AutoIdentifyEPSG()
	assert.NoError(t, err)

	epsg32632, err := NewSpatialRefFromEPSG(32632)
	assert.NoError(t, err)
	assert.NoError(t, sr.Validate())
	assert.True(t, epsg32632.Projected())
	assert.False(t, epsg32632.EPSGTreatsAsLatLong())

	l, err := NewSpatialRefFromWKT(`LOCAL_CS[,UNIT["m",1]]`)
	assert.NoError(t, err)
	ehc := eh()
	assert.Error(t, l.Validate(ErrLogger(ehc.ErrorHandler)))
	err = l.AutoIdentifyEPSG()
	assert.Error(t, err)
	_, err = l.SemiMajor()
	assert.Error(t, err)
	_, err = l.SemiMinor()
	assert.Error(t, err)

	//TODO? Find a better way to mak WKT() error out
	l = &SpatialRef{}
	_, err = l.WKT()
	assert.Error(t, err)
}

func TestGeoTransform(t *testing.T) {
	tmpname := tempfile()
	defer os.Remove(tmpname)
	ds, err := Create(GTiff, tmpname, 1, Byte, 20, 20)
	assert.NoError(t, err)
	defer ds.Close()
	_, err = ds.GeoTransform()
	assert.Error(t, err)
	ehc := eh()
	_, err = ds.GeoTransform(ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	_, err = ds.Bounds()
	assert.Error(t, err)
	ngt := [6]float64{0, 2, 1, 0, 1, 1}

	err = ds.SetGeoTransform(ngt)
	assert.NoError(t, err)
	ehc = eh()
	err = ds.SetGeoTransform(ngt, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	gt, err := ds.GeoTransform()
	assert.NoError(t, err)
	assert.Equal(t, gt, ngt)
	ehc = eh()
	_, err = ds.GeoTransform(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
}

func TestGeometryTransform(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	srm, _ := NewSpatialRefFromEPSG(3857)
	gp, _ := NewGeometryFromWKT("POINT (10 10)", sr)
	assert.True(t, gp.SpatialRef().IsSame(sr))

	err := gp.Reproject(srm)
	assert.NoError(t, err)
	assert.True(t, gp.SpatialRef().IsSame(srm))
	gp.Close()

	ehc := eh()
	gp, _ = NewGeometryFromWKT("POINT (10 10)", sr)
	err = gp.Reproject(srm, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	nwkt, _ := gp.WKT()
	assert.NotEqual(t, "POINT (10 10)", nwkt)
	gp.SetSpatialRef(sr)
	assert.True(t, gp.SpatialRef().IsSame(sr))

	gp.Close()

	gp, _ = NewGeometryFromWKT("POINT (10 91)", sr)
	err = gp.Reproject(srm)
	assert.Error(t, err)
	gp.Close()

	ehc = eh()
	gp, _ = NewGeometryFromWKT("POINT (10 91)", sr, ErrLogger(ehc.ErrorHandler))
	err = gp.Reproject(srm)
	assert.Error(t, err)
	gp.Close()

	trn, _ := NewTransform(sr, srm)
	gp, _ = NewGeometryFromWKT("POINT (10 10)", nil)

	err = gp.Transform(trn)
	assert.NoError(t, err)
	assert.True(t, gp.SpatialRef().IsSame(srm))
	nwkt, _ = gp.WKT()
	assert.NotEqual(t, "POINT (10 10)", nwkt)
	gp.Close()

	ehc = eh()
	gp, _ = NewGeometryFromWKT("POINT (10 10)", nil)
	err = gp.Transform(trn, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	gp.Close()

	gp, _ = NewGeometryFromWKT("POINT (10 91)", sr)
	err = gp.Transform(trn)
	assert.Error(t, err)
	gp.Close()

	ehc = eh()
	gp, _ = NewGeometryFromWKT("POINT (10 91)", sr)
	err = gp.Transform(trn, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	gp.Close()
}

func TestProjBounds(t *testing.T) {
	sr4326, _ := NewSpatialRefFromEPSG(4326)
	sr3857, _ := NewSpatialRefFromEPSG(3857)
	box, err := NewGeometryFromWKT("POLYGON((-180 -91,-180 90,180 90,180 -90,-180 -91))", sr4326)
	assert.NoError(t, err)
	_, err = box.Bounds(sr3857)
	assert.Error(t, err)
	_, err = box.Bounds(&SpatialRef{handle: nil})
	assert.Error(t, err)

}

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
	assert.Error(t, err, "invalid creation option not detected")
	ehc := eh()
	_, err = ds.Translate(tmpname2, nil, CreationOption("BAR=BAZ"), ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err, "invalid creation option not detected")

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
	ds1, _ := Create(Memory, "", 1, Byte, 5, 5)
	ds2, _ := Create(Memory, "", 1, Byte, 5, 5)

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

	if outputDataset.Structure().SizeX != 10 || outputDataset.Structure().SizeY != 5 {
		t.Errorf("wrong size %d,%d", outputDataset.Structure().SizeX, outputDataset.Structure().SizeY)
	}

	// read total warp result
	data := make([]uint8, 50)
	err = outputDataset.Read(0, 0, data, outputDataset.Structure().SizeX, outputDataset.Structure().SizeY) //Bands(0, 1, 2),
	//Window(outputDataset.Structure().SizeX, outputDataset.Structure().SizeY),

	assert.NoError(t, err)

	assert.Equal(t, []uint8{
		200, 200, 200, 200, 200, 100, 100, 100, 100, 100,
		200, 200, 200, 200, 200, 100, 100, 100, 100, 100,
		200, 200, 200, 200, 200, 100, 100, 100, 100, 100,
		200, 200, 200, 200, 200, 100, 100, 100, 100, 100,
		200, 200, 200, 200, 200, 100, 100, 100, 100, 100}, data)
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
	assert.Error(t, err, "creation option option should have raised an error")
	ehc := eh()
	err = outputDataset.WarpInto([]*Dataset{inputDataset}, []string{"-co", "TILED=YES"}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err, "creation option option should have raised an error")

	if err = outputDataset.WarpInto([]*Dataset{inputDataset}, []string{}); err != nil {
		t.Fatal(err)
	}
	ehc = eh()
	assert.NoError(t, outputDataset.WarpInto([]*Dataset{inputDataset}, []string{}, ErrLogger(ehc.ErrorHandler)))

	data := make([]uint8, 1)
	_ = outputDataset.Read(0, 0, data, 1, 1)
	assert.Equal(t, uint8(155), data[0])
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
	assert.NoError(t, err)
	ehc := eh()
	err = ds.ClearOverviews(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	err = ds.BuildOverviews(MinSize(200))
	assert.NoError(t, err)
	ehc = eh()
	err = ds.BuildOverviews(MinSize(200), ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

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

	_ = ds.ClearOverviews()
	err = ds.BuildOverviews(ConfigOption("GDAL_TIFF_OVR_BLOCKSIZE=64"))
	assert.NoError(t, err)
	ovrst := ds.Bands()[0].Overviews()[0].Structure()
	assert.Equal(t, 64, ovrst.BlockSizeX)

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
	assert.Error(t, err, "invalid field not raised")
	ehc := eh()
	err = bnd.Polygonize(pl4, PixelValueFieldIndex(5), ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err, "invalid field not raised")

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

func TestFillNoData(t *testing.T) {
	ds, _ := Create(Memory, "", 1, Byte, 1000, 1000)
	mskds, _ := Create(Memory, "", 1, Byte, 1000, 1000)
	defer ds.Close()
	defer mskds.Close()
	_ = ds.SetNoData(0)
	bnd := ds.Bands()[0]
	msk := mskds.Bands()[0]
	_ = bnd.Fill(0, 0)
	_ = msk.Fill(255, 0)
	buf := make([]byte, 100)
	for i := range buf {
		buf[i] = 128
	}
	_ = bnd.Write(495, 495, buf, 10, 10)
	for i := range buf {
		buf[i] = 0
	}
	_ = msk.Write(495, 495, buf, 10, 10)

	err := bnd.FillNoData()
	assert.NoError(t, err)

	// test that the default 100 pixel distance is respected
	_ = bnd.Read(500, 595, buf, 10, 10)
	assert.Equal(t, uint8(128), buf[0])
	assert.Equal(t, uint8(0), buf[99])

	_ = bnd.Fill(0, 0)
	for i := range buf {
		buf[i] = 128
	}
	_ = bnd.Write(495, 495, buf, 10, 10)

	err = bnd.FillNoData(MaxDistance(10))
	assert.NoError(t, err)
	// test that the 10 pixel distance is respected
	_ = bnd.Read(500, 595, buf, 10, 10)
	assert.Equal(t, uint8(0), buf[0])
	assert.Equal(t, uint8(0), buf[99])
	_ = bnd.Read(510, 510, buf, 10, 10)
	assert.Equal(t, uint8(128), buf[0])
	assert.Equal(t, uint8(0), buf[99])

	//test that output is changed when smoothing is applied
	//smoothing is only visible on the horizontal/vertical cross
	//centered on the data patch
	_ = bnd.Fill(0, 0)
	for i := range buf {
		buf[i] = byte(i + 23)
	}
	_ = bnd.Write(495, 495, buf, 10, 10)
	_ = bnd.FillNoData()
	val1 := make([]byte, 1)
	_ = bnd.Read(520, 500, val1, 1, 1)

	_ = bnd.Fill(0, 0)
	_ = bnd.Write(495, 495, buf, 10, 10)
	_ = bnd.FillNoData(SmoothingIterations(20))
	val2 := make([]byte, 1)
	_ = bnd.Read(520, 500, val2, 1, 1)
	assert.NotEqual(t, val1[0], val2[0])

	//test masked.
	_ = bnd.Fill(0, 0)
	for i := range buf {
		buf[i] = byte(i + 23)
	}
	_ = bnd.Write(495, 495, buf, 10, 10)
	_ = bnd.Read(500, 500, val1, 1, 1)
	_ = bnd.FillNoData(Mask(msk))
	_ = bnd.Read(500, 500, val2, 1, 1)
	assert.NotEqual(t, val1[0], val2[0])

	ehc := eh()
	nilbnd := Band{}
	err = nilbnd.FillNoData(ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	assert.Equal(t, 1, ehc.errs)
}

/*
func debug(ds *Dataset) {
	str := ds.Structure()
	tmpf, _ := ioutil.TempFile("", "godal*.tif")
	tmpf.Close()
	dds, _ := Create(GTiff, tmpf.Name(), str.NBands, str.DataType, str.SizeX, str.SizeY)
	buf := make([]byte, str.NBands*str.SizeX*str.SizeY)
	_ = ds.Read(0, 0, buf, str.SizeX, str.SizeY)
	_ = dds.Write(0, 0, buf, str.SizeX, str.SizeY)
	dds.Close()
	fmt.Fprintln(os.Stderr, tmpf.Name())
}
*/

func TestRasterize(t *testing.T) {
	tf := tempfile()
	defer os.Remove(tf)
	inv, _ := Open("testdata/test.geojson", VectorOnly())

	_, err := inv.Rasterize(tf, []string{"-of", "bogus"})
	assert.Error(t, err)
	ehc := eh()
	_, err = inv.Rasterize(tf, []string{"-of", "bogus"}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	dl := &debugLogger{}
	rds, err := inv.Rasterize(tf, []string{
		"-te", "99", "-1", "102", "2",
		"-ts", "9", "9",
		"-init", "10",
		"-burn", "20"}, CreationOption("TILED=YES"), GTiff, ErrLogger(dl.L), ConfigOption("CPL_DEBUG=ON"))
	assert.NoError(t, err)
	assert.NotEmpty(t, dl.logs)
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

func TestRasterizeInto(t *testing.T) {
	vds, _ := Open("testdata/test.geojson")
	//ext is 100,0,101,1
	defer vds.Close()
	mds, err := Create(Memory, "", 3, Byte, 3, 3)
	assert.NoError(t, err)
	defer mds.Close()
	_ = mds.SetGeoTransform([6]float64{99.1, 1, 0, 1.9, 0, -1}) //set extent to 99.1,-0.9,102.1,1.9
	bnds := mds.Bands()

	for _, bnd := range bnds {
		_ = bnd.Fill(255, 0)
	}
	data := make([]byte, 27) //to extract a 3x3 window

	assert.Error(t, mds.RasterizeInto(vds, nil)) //missing srs
	ehc := eh()
	assert.Error(t, mds.RasterizeInto(vds, nil, ErrLogger(ehc.ErrorHandler)))

	sr, err := NewSpatialRefFromEPSG(4326)
	assert.NoError(t, err)
	assert.NoError(t, mds.SetSpatialRef(sr))
	assert.NoError(t, mds.RasterizeInto(vds, []string{"-burn", "0"}))

	_ = mds.Read(0, 0, data, 3, 3)
	assert.Equal(t, []byte{255, 255, 255}, data[0:3])
	assert.Equal(t, []byte{0, 255, 255}, data[12:15])
	assert.Equal(t, []byte{255, 255, 255}, data[24:27])

	for _, bnd := range bnds {
		_ = bnd.Fill(255, 0)
	}
	assert.NoError(t, mds.RasterizeInto(vds, []string{"-burn", "0", "-at"}))

	_ = mds.Read(0, 0, data, 3, 3)
	assert.Equal(t, []byte{0, 255, 255}, data[0:3])
	assert.Equal(t, []byte{0, 255, 255}, data[12:15])
	assert.Equal(t, []byte{255, 255, 255}, data[24:27])

}
func TestRasterizeGeometries(t *testing.T) {
	vds, _ := Open("testdata/test.geojson")
	//ext is 100,0,101,1
	defer vds.Close()
	mds, _ := Create(Memory, "", 3, Byte, 3, 3)
	defer mds.Close()
	_ = mds.SetGeoTransform([6]float64{99.1, 1, 0, 1.9, 0, -1}) //set extent to 99.1,-0.9,102.1,1.9
	bnds := mds.Bands()

	ff := vds.Layers()[0].NextFeature().Geometry()

	for _, bnd := range bnds {
		_ = bnd.Fill(255, 0)
	}
	data := make([]byte, 27) //to extract a 3x3 window

	err := mds.RasterizeGeometry(ff)
	assert.NoError(t, err)
	_ = mds.Read(0, 0, data, 3, 3)
	assert.Equal(t, []byte{255, 255, 255}, data[0:3])
	assert.Equal(t, []byte{0, 0, 0}, data[12:15])
	assert.Equal(t, []byte{255, 255, 255}, data[24:27])

	alldata1 := make([]byte, 3*3*3)
	_ = mds.Read(0, 0, alldata1, 3, 3)
	alldata2 := make([]byte, 3*3*3)

	//with alltouched we will light up more than just the center pixel
	err = mds.RasterizeGeometry(ff, AllTouched())
	assert.NoError(t, err)
	_ = mds.Read(0, 0, alldata2, 3, 3)
	assert.NotEqual(t, alldata1, alldata2)

	err = mds.RasterizeGeometry(ff, Values(200))
	assert.NoError(t, err)
	_ = mds.Read(0, 0, data, 3, 3)
	assert.Equal(t, []byte{200, 200, 200}, data[12:15])

	err = mds.RasterizeGeometry(ff, Bands(0), Values(100))
	assert.NoError(t, err)
	_ = mds.Read(0, 0, data, 3, 3)
	assert.Equal(t, []byte{100, 200, 200}, data[12:15])

	err = mds.RasterizeGeometry(ff, Values(1, 2, 3))
	assert.NoError(t, err)
	_ = mds.Read(0, 0, data, 3, 3)
	assert.Equal(t, []uint8{1, 2, 3}, data[12:15])

	err = mds.RasterizeGeometry(ff, Bands(0, 1), Values(5, 6))
	assert.NoError(t, err)
	_ = mds.Read(0, 0, data, 3, 3)
	assert.Equal(t, []uint8{5, 6, 3}, data[12:15])

	err = mds.RasterizeGeometry(ff, Bands(0), Values(1, 2))
	assert.Error(t, err)
	err = mds.RasterizeGeometry(ff, Bands(0, 2, 3), Values(1, 2, 3))
	assert.Error(t, err)
	ehc := eh()
	err = mds.RasterizeGeometry(ff, Bands(0, 2, 3), Values(1, 2, 3), ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

}

func TestVectorTranslate(t *testing.T) {
	tmpname := tempfile()
	defer os.Remove(tmpname)
	ds, err := Open("testdata/test.geojson", VectorOnly())
	assert.NoError(t, err)

	st1, _ := os.Stat("testdata/test.geojson")
	nds, err := ds.VectorTranslate(tmpname, []string{"-lco", "RFC7946=YES"}, GeoJSON)
	assert.NoError(t, err)

	_ = nds.SetMetadata("baz", "boo")
	err = nds.Close()
	assert.NoError(t, err)

	st2, _ := os.Stat(tmpname)
	if st2.Size() == 0 || st1.Size() == st2.Size() {
		t.Error("invalid size")
	}

	err = RegisterVector("TAB")
	assert.NoError(t, err)

	tmpdir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(tmpdir)
	dl := &debugLogger{}
	mds, err := ds.VectorTranslate(filepath.Join(tmpdir, "test.mif"), []string{"-f", "Mapinfo File"}, CreationOption("FORMAT=MIF"),
		ErrLogger(dl.L), ConfigOption("CPL_DEBUG=ON"))
	assert.NoError(t, err)
	assert.NotEmpty(t, dl.logs)
	mds.Close()

	_, err = ds.VectorTranslate("foobar", []string{"-f", "bogusdriver"})
	assert.Error(t, err)
	ehc := eh()
	_, err = ds.VectorTranslate("foobar", []string{"-f", "bogusdriver"}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
}

func TestVectorLayer(t *testing.T) {
	rds, _ := Create(Memory, "", 3, Byte, 10, 10)
	_, err := rds.CreateLayer("ff", nil, GTPolygon)
	assert.Error(t, err)
	ehc := eh()
	_, err = rds.CreateLayer("ff", nil, GTPolygon, ErrLogger(ehc.ErrorHandler))

	assert.Error(t, err)
	lyrs := rds.Layers()
	if len(lyrs) > 0 {
		t.Error("raster ds has vector layers")
	}
	rds.Close()
	tmpname := tempfile()
	defer os.Remove(tmpname)
	ds, err := Open("testdata/test.geojson", VectorOnly())
	if err != nil {
		t.Fatal(err)
	}
	assert.Nil(t, ds.Bands())
	assert.Error(t, ds.BuildOverviews())
	assert.Error(t, ds.ClearOverviews())
	ehc = eh()
	assert.Error(t, ds.ClearOverviews(ErrLogger(ehc.ErrorHandler)))
	assert.Error(t, ds.SetNoData(0))
	ehc = eh()
	assert.Error(t, ds.SetNoData(0, ErrLogger(ehc.ErrorHandler)))
	buf := make([]byte, 10)
	ehc = eh()
	assert.Error(t, ds.Read(0, 0, buf, 3, 3))
	assert.Error(t, ds.Read(0, 0, buf, 3, 3, ErrLogger(ehc.ErrorHandler)))
	ehc = eh()
	assert.Error(t, ds.Write(0, 0, buf, 3, 3))
	assert.Error(t, ds.Write(0, 0, buf, 3, 3, ErrLogger(ehc.ErrorHandler)))

	sr3857, _ := NewSpatialRefFromEPSG(3857)
	defer sr3857.Close()

	layer := ds.Layers()[0]
	assert.Equal(t, layer.Name(), "test")
	assert.Equal(t, layer.Type(), GTPolygon)
	bounds, err := layer.Bounds()
	assert.NoError(t, err)
	assert.Equal(t, bounds, [4]float64{100, 0, 101, 1})
	_, err = layer.Bounds(sr3857)
	assert.NoError(t, err)
	_, err = layer.Bounds(&SpatialRef{})
	assert.Error(t, err)
	_, err = Layer{}.Bounds(ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	assert.Nil(t, ds.LayerByName("none"))
	testLayer := ds.LayerByName("test")
	assert.NotNil(t, testLayer)
	vds, _ := CreateVector(Memory, "")
	copiedLayer, err := vds.CopyLayer(*testLayer, "copied")
	assert.NoError(t, err)
	testLayer.ResetReading()
	feature := testLayer.NextFeature()
	err = copiedLayer.CreateFeature(feature)
	assert.NoError(t, err)
	err = copiedLayer.CreateFeature(&Feature{}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	_, err = vds.CopyLayer(Layer{}, "empty", ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	_ = vds.Close()

	dds, err := ds.VectorTranslate("", []string{"-of", "MEMORY"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = (&Geometry{}).Buffer(10, 1)
	assert.Error(t, err)
	ehc = eh()
	_, err = (&Geometry{}).Buffer(10, 1, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	_, err = (&Geometry{}).Simplify(1)
	assert.Error(t, err)
	ehc = eh()
	_, err = (&Geometry{}).Simplify(1, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	err = (&Feature{}).SetGeometry(&Geometry{})
	assert.Error(t, err)
	ehc = eh()
	err = (&Feature{}).SetGeometry(&Geometry{}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	sr4326, _ := NewSpatialRefFromEPSG(4326)
	defer sr4326.Close()
	l2, err := dds.CreateLayer("t2", sr4326, GTPoint)
	assert.NoError(t, err)
	assert.True(t, sr4326.IsSame(l2.SpatialRef()))
	l := dds.Layers()[0]
	l.ResetReading()
	_, err = l.FeatureCount()
	assert.NoError(t, err)
	_, err = Layer{}.FeatureCount()
	assert.Error(t, err)
	ehc = eh()
	cnt, err := l.FeatureCount(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	ehc = eh()
	_, err = Layer{}.FeatureCount(ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	i := 0
	for {
		ff := l.NextFeature()
		if ff == nil {
			break
		}
		i++
		og := ff.Geometry()
		if i == 1 {
			bounds, _ := og.Bounds()
			assert.Equal(t, [4]float64{100, 0, 101, 1}, bounds)
			b3857, err := og.Bounds(sr3857)
			assert.NoError(t, err)
			assert.NotEqual(t, bounds, b3857)
		}
		bg, err := og.Buffer(0.01, 1)
		assert.NoError(t, err)
		og.Close()
		sg, err := bg.Simplify(0.01)
		assert.NoError(t, err)
		bg.Close()
		assert.NotPanics(t, bg.Close, "2nd geom close must not panic")
		err = ff.SetGeometry(sg)
		assert.NoError(t, err)

		em, err := sg.Buffer(-200, 1)
		assert.NoError(t, err)
		if !em.Empty() {
			t.Error("-200 buf not empty")
		}

		em.Close()
		sg.Close()
		err = l.UpdateFeature(ff)
		assert.NoError(t, err)
		ehc = eh()
		err = l.UpdateFeature(ff, ErrLogger(ehc.ErrorHandler))
		assert.NoError(t, err)
		ff.Close()
		assert.NotPanics(t, ff.Close, "second close must not panic")
	}
	if i != 2 || i != cnt {
		t.Error("wrong feature count")
	}
	err = dds.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestLayerModifyFeatures(t *testing.T) {
	ds, _ := Open("testdata/test.geojson") //read-only
	defer ds.Close()
	l := ds.Layers()[0]
	for {
		ff := l.NextFeature()
		if ff == nil {
			break
		}
		err := l.DeleteFeature(ff)
		assert.Error(t, err) //read-only, must fail
		ehc := eh()
		err = l.DeleteFeature(ff, ErrLogger(ehc.ErrorHandler))
		assert.Error(t, err) //read-only, must fail
		err = l.UpdateFeature(ff)
		assert.Error(t, err) //read-only, must fail
		ehc = eh()
		err = l.UpdateFeature(ff, ErrLogger(ehc.ErrorHandler))
		assert.Error(t, err) //read-only, must fail
	}
	dsm, _ := ds.VectorTranslate("", []string{"-of", "Memory"})
	defer dsm.Close()
	l = dsm.Layers()[0]
	for {
		ff := l.NextFeature()
		if ff == nil {
			break
		}
		err := l.DeleteFeature(ff)
		assert.NoError(t, err) //read-write, must not fail
	}
	c, _ := l.FeatureCount()
	assert.Equal(t, 0, c)

}

func TestNewGeometry(t *testing.T) {
	_, err := NewGeometryFromWKT("babsaba", &SpatialRef{})
	assert.Error(t, err)
	ehc := eh()
	_, err = NewGeometryFromWKT("babsaba", &SpatialRef{}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	gp, err := NewGeometryFromWKT("POINT (30 10)", nil)
	assert.NoError(t, err)
	assert.NotNil(t, gp)
	ehc = eh()
	gp, err = NewGeometryFromWKT("POINT (30 10)", nil, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	wkt, err := gp.WKT()
	assert.NoError(t, err)
	assert.Equal(t, "POINT (30 10)", wkt)
	ehc = eh()
	_, err = gp.WKT(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	wkb, err := gp.WKB()
	assert.NoError(t, err)
	assert.NotEmpty(t, wkb)
	ehc = eh()
	_, err = gp.WKB(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	gp.Close()

	_, err = NewGeometryFromWKB(wkb[0:10], &SpatialRef{})
	assert.Error(t, err)
	ehc = eh()
	_, err = NewGeometryFromWKB(wkb[0:10], &SpatialRef{}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	gp, err = NewGeometryFromWKB(wkb, nil)
	assert.NoError(t, err)
	assert.NotNil(t, gp)
	ehc = eh()
	gp, err = NewGeometryFromWKB(wkb, nil, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	assert.NotNil(t, gp)

	wkt, err = gp.WKT()
	assert.NoError(t, err)
	assert.Equal(t, "POINT (30 10)", wkt)

	_, err = (&Geometry{}).WKB()
	assert.Error(t, err)

	_, err = (&Geometry{}).WKT()
	assert.Error(t, err)
}

func TestNewGeometryFromGeoJSON(t *testing.T) {
	jsonStr := `{ "type": "Polygon", "coordinates": [ [ [ -71.7, 44.9 ], [ -71.8, 45.1 ], [ -71.6, 45.2 ], [ -70.6, 45.3 ], [ -71.7, 44.9 ] ] ] }`

	_, err := NewGeometryFromGeoJSON("babsaba")
	assert.Error(t, err)
	ehc := eh()
	_, err = NewGeometryFromGeoJSON("babsaba", ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	gp, err := NewGeometryFromGeoJSON(jsonStr)
	assert.NoError(t, err)
	assert.NotNil(t, gp)
	ehc = eh()
	gp, err = NewGeometryFromGeoJSON(jsonStr, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	outJSON, err := gp.GeoJSON()
	assert.NoError(t, err)
	assert.Equal(t, jsonStr, outJSON)
}

func TestGeometryDifference(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	defer sr.Close()

	polyStr := "POLYGON ((0 0,2 0,2 2,0 2,0 0))"
	polyGeom1, _ := NewGeometryFromWKT(polyStr, sr)
	polyStr = "POLYGON ((0 0,1 0,1 1,0 1,0 0))"
	polyGeom2, _ := NewGeometryFromWKT(polyStr, sr)

	diffGeom, err := polyGeom1.Difference(polyGeom2)
	assert.NoError(t, err)
	assert.Equal(t, diffGeom.Area(), 3.0)

	_, err = polyGeom1.Difference(&Geometry{})
	assert.Error(t, err)

	ehc := eh()
	_, err = (&Geometry{}).Difference(polyGeom2, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
}

func TestGeometryIntersection(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	defer sr.Close()

	polyStr := "POLYGON ((0 0,2 0,2 2,0 2,0 0))"
	polyGeom1, _ := NewGeometryFromWKT(polyStr, sr)
	polyStr = "POLYGON ((1 1,3 1,3 3,1 3,1 1))"
	polyGeom2, _ := NewGeometryFromWKT(polyStr, sr)

	intersectionGeom, err := polyGeom1.Intersection(polyGeom2)
	assert.NoError(t, err)
	assert.Equal(t, intersectionGeom.Area(), 1.0)

	_, err = polyGeom1.Intersection(&Geometry{})
	assert.Error(t, err)

	ehc := eh()
	_, err = (&Geometry{}).Intersection(polyGeom2, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
}

func TestGeometryUnion(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	defer sr.Close()

	polyStr := "POLYGON ((0 0,2 0,2 2,0 2,0 0))"
	polyGeom1, _ := NewGeometryFromWKT(polyStr, sr)
	polyStr = "POLYGON ((1 1,3 1,3 3,1 3,1 1))"
	polyGeom2, _ := NewGeometryFromWKT(polyStr, sr)

	unionGeom, err := polyGeom1.Union(polyGeom2)
	assert.NoError(t, err)
	assert.Equal(t, unionGeom.Area(), 7.0)

	_, err = polyGeom1.Union(&Geometry{})
	assert.Error(t, err)

	ehc := eh()
	_, err = (&Geometry{}).Union(polyGeom2, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
}

func TestGeometryIntersects(t *testing.T) {
	_, err := (&Geometry{}).Intersects(&Geometry{})
	assert.Error(t, err)

	ehc := eh()
	_, err = (&Geometry{}).Intersects(&Geometry{}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	poly1Str := `{ "type": "Polygon", "coordinates": [ [ [ 0, 0 ], [ 1, 0 ], [ 1, 1 ], [ 0, 1 ], [ 0, 0 ] ] ] }`
	poly2Str := `{ "type": "Polygon", "coordinates": [ [ [ 2, 0 ], [ 3, 0 ], [ 3, 1 ], [ 2, 1 ], [ 2, 0 ] ] ] }`

	gp1, err := NewGeometryFromGeoJSON(poly1Str)
	assert.NoError(t, err)
	assert.NotNil(t, gp1)

	gp2, err := NewGeometryFromGeoJSON(poly2Str)
	assert.NoError(t, err)
	assert.NotNil(t, gp2)

	_, err = gp1.Intersects(&Geometry{})
	assert.Error(t, err)

	_, err = (&Geometry{}).Intersects(gp1)
	assert.Error(t, err)

	ret, err := gp1.Intersects(gp1)
	assert.NoError(t, err)
	assert.True(t, ret)

	ehc = eh()
	ret, err = gp1.Intersects(gp1, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	assert.True(t, ret)

	ret, err = gp1.Intersects(gp2)
	assert.NoError(t, err)
	assert.False(t, ret)

	ehc = eh()
	ret, err = gp1.Intersects(gp2, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	assert.False(t, ret)
}

func TestGeomToGeoJSON(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	g, _ := NewGeometryFromWKT("POINT (10.123456789 10)", sr)
	gj, err := g.GeoJSON()
	assert.NoError(t, err)
	assert.Equal(t, `{ "type": "Point", "coordinates": [ 10.1234568, 10.0 ] }`, gj)

	gj, err = g.GeoJSON(SignificantDigits(3))
	assert.NoError(t, err)
	assert.Equal(t, `{ "type": "Point", "coordinates": [ 10.123, 10.0 ] }`, gj)

	_, err = (&Geometry{}).GeoJSON()
	assert.Error(t, err)
	ehc := eh()
	_, err = (&Geometry{}).GeoJSON(ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

}

func TestGeometryToGML(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	defer sr.Close()

	polyStr := "POLYGON ((0 0,2 0,2 2,0 2,0 0))"
	polyGeom, _ := NewGeometryFromWKT(polyStr, sr)

	gml, err := polyGeom.GML()
	assert.NoError(t, err)
	assert.Equal(t, gml, `<gml:Polygon srsName="EPSG:4326"><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>0,0 2,0 2,2 0,2 0,0</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>`)

	gml, err = polyGeom.GML(CreationOption("FORMAT=GML3", "SRSNAME_FORMAT=OGC_URN"))
	assert.NoError(t, err)
	assert.Equal(t, gml, `<gml:Polygon srsName="urn:ogc:def:crs:EPSG::4326"><gml:exterior><gml:LinearRing><gml:posList>0 0 0 2 2 2 2 0 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>`)

	ehc := eh()
	_, err = polyGeom.GML(CreationOption("FORMAT=GML3", "SRSNAME_FORMAT=fake"), ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
}

func TestMultiPolygonGeometry(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	defer sr.Close()

	multiPolyStr := "MULTIPOLYGON(((1 1,5 1,5 5,1 5,1 1),(2 2,2 3,3 3,3 2,2 2)),((6 3,9 2,9 4,6 3)))"
	multiPolyGeom, _ := NewGeometryFromWKT(multiPolyStr, sr)

	assert.Equal(t, multiPolyGeom.Area(), 18.0)
	assert.Equal(t, multiPolyGeom.GeometryCount(), 2)
	assert.Equal(t, multiPolyGeom.Name(), "MULTIPOLYGON")
	assert.Equal(t, multiPolyGeom.Type(), GTMultiPolygon)

	subGeom, err := multiPolyGeom.SubGeometry(0)
	assert.NoError(t, err)
	wkt, _ := subGeom.WKT()
	assert.Equal(t, wkt, "POLYGON ((1 1,5 1,5 5,1 5,1 1),(2 2,2 3,3 3,3 2,2 2))")
	subGeom, err = multiPolyGeom.SubGeometry(1)
	assert.NoError(t, err)
	wkt, _ = subGeom.WKT()
	assert.Equal(t, wkt, "POLYGON ((6 3,9 2,9 4,6 3))")
	ehc := eh()
	_, err = multiPolyGeom.SubGeometry(2, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	polyGeom := multiPolyGeom.ForceToPolygon()
	wkt, _ = polyGeom.WKT()
	assert.Equal(t, wkt, "POLYGON ((1 1,5 1,5 5,1 5,1 1),(2 2,2 3,3 3,3 2,2 2),(6 3,9 2,9 4,6 3))")
	assert.False(t, polyGeom.Valid())

	polyStr := "POLYGON((1 1,5 1,5 5,1 5,1 1))"
	polyGeom, _ = NewGeometryFromWKT(polyStr, sr)
	multiPolyGeom = polyGeom.ForceToMultiPolygon()
	wkt, _ = multiPolyGeom.WKT()
	assert.Equal(t, wkt, "MULTIPOLYGON (((1 1,5 1,5 5,1 5,1 1)))")
	assert.True(t, polyGeom.Valid())

	multiPolyStr = "MULTIPOLYGON (((1 1,5 1,5 5,1 5,1 1)))"
	multiPolyGeom, _ = NewGeometryFromWKT(multiPolyStr, sr)
	polyStr = "POLYGON((6 3,9 2,9 4,6 3))"
	polyGeom, _ = NewGeometryFromWKT(polyStr, sr)
	assert.False(t, multiPolyGeom.Contains(polyGeom))
	err = multiPolyGeom.AddGeometry(polyGeom, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	wkt, _ = multiPolyGeom.WKT()
	assert.Equal(t, wkt, "MULTIPOLYGON (((1 1,5 1,5 5,1 5,1 1)),((6 3,9 2,9 4,6 3)))")
}

func TestFeatureAttributes(t *testing.T) {
	glayers := `{
	"type": "FeatureCollection",
	"features": [
		{
			"type": "Feature",
			"properties": {
				"strCol":"foobar",
				"intCol":3,
				"floatCol":123.4,
				"dateCol":"2006-01-02T15:04:05",
			},
			"geometry": {
				"type": "Point",
				"coordinates": [1,1]
			}
		}
	]
}`
	ds, _ := Open(glayers, VectorOnly())
	lyr := ds.Layers()[0]

	//trying to make this fail "cleanly", but not managing. using a null layer for this
	//curve, err := NewGeometryFromWKT("CURVEPOLYGON(COMPOUNDCURVE(CIRCULARSTRING (0 0,1 1,2 0),(2 0,0 0)))", nil)
	//assert.NoError(t, err)
	_, err := (&Layer{}).NewFeature(&Geometry{})
	assert.Error(t, err)
	ehc := eh()
	_, err = (&Layer{}).NewFeature(&Geometry{}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	dateFormatRFC3339 := "2006-01-02T15:04:05Z07:00"

	i := 0
	for {
		f := lyr.NextFeature()
		if f == nil {
			break
		}
		attrs := f.Fields()
		switch i {
		case 0:
			_, ok := attrs["foo"]
			assert.False(t, ok)
			sfield := attrs["strCol"]
			assert.Equal(t, "foobar", sfield.String())
			assert.Equal(t, int64(0), sfield.Int())
			assert.Equal(t, 0.0, sfield.Float())
			sfield = attrs["intCol"]
			assert.Equal(t, "3", sfield.String())
			assert.Equal(t, int64(3), sfield.Int())
			assert.Equal(t, 3.0, sfield.Float())
			sfield = attrs["floatCol"]
			assert.Equal(t, "123.400000", sfield.String())
			assert.Equal(t, int64(123), sfield.Int())
			assert.Equal(t, 123.4, sfield.Float())
			sfield = attrs["dateCol"]
			assert.Equal(t, "2006-01-02T15:04:05Z", sfield.DateTime().Format(dateFormatRFC3339))
		}
		i++
	}
	_ = ds.Close()

	ds, _ = CreateVector(Memory, "")
	lyr, err = ds.CreateLayer("l1", nil, GTPoint,
		NewFieldDefinition("strCol", FTString),
		NewFieldDefinition("intCol", FTInt),
		NewFieldDefinition("int64Col", FTInt64),
		NewFieldDefinition("floatCol", FTReal),
		NewFieldDefinition("intListCol", FTIntList),
		NewFieldDefinition("int64ListCol", FTInt64List),
		NewFieldDefinition("floatListCol", FTRealList),
		NewFieldDefinition("stringListCol", FTStringList),
		NewFieldDefinition("binaryCol", FTBinary),
		NewFieldDefinition("dateCol", FTDate),
		NewFieldDefinition("timeCol", FTTime),
		NewFieldDefinition("dateTimeCol", FTDateTime),
		NewFieldDefinition("unknownCol", FTUnknown),
	)
	assert.NoError(t, err)

	calcuttaLoc, err := time.LoadLocation("Asia/Calcutta")
	assert.NoError(t, err)

	pnt, _ := NewGeometryFromWKT("POINT (1 1)", nil)
	nf, err := lyr.NewFeature(pnt)
	assert.NoError(t, err)
	fc, _ := lyr.FeatureCount()
	assert.Equal(t, fc, 1)
	nf.SetGeometryColumnName("no_error")
	nf.SetFID(99999999999)
	attrs := nf.Fields()
	intCol := attrs["intCol"]
	intCol.ftype = FTString
	assert.Error(t, nf.SetFieldValue(intCol, "not_int", ErrLogger(ehc.ErrorHandler)))
	assert.Error(t, nf.SetFieldValue(attrs["strCol"], 0))
	assert.Error(t, nf.SetFieldValue(attrs["intCol"], ""))
	assert.Error(t, nf.SetFieldValue(attrs["int64Col"], ""))
	assert.Error(t, nf.SetFieldValue(attrs["floatCol"], ""))
	assert.Error(t, nf.SetFieldValue(attrs["intListCol"], ""))
	assert.Error(t, nf.SetFieldValue(attrs["int64ListCol"], ""))
	assert.Error(t, nf.SetFieldValue(attrs["floatListCol"], ""))
	assert.Error(t, nf.SetFieldValue(attrs["stringListCol"], ""))
	assert.Error(t, nf.SetFieldValue(attrs["binaryCol"], ""))
	assert.Error(t, nf.SetFieldValue(attrs["dateCol"], ""))
	assert.Error(t, nf.SetFieldValue(attrs["timeCol"], ""))
	assert.Error(t, nf.SetFieldValue(attrs["dateTimeCol"], ""))
	assert.Error(t, nf.SetFieldValue(attrs["unknownCol"], ""))
	assert.NoError(t, nf.SetFieldValue(attrs["strCol"], "foo"))
	assert.NoError(t, nf.SetFieldValue(attrs["intCol"], 1))
	assert.NoError(t, nf.SetFieldValue(attrs["int64Col"], int64(2)))
	assert.NoError(t, nf.SetFieldValue(attrs["floatCol"], 3.0))
	assert.NoError(t, nf.SetFieldValue(attrs["intListCol"], []int{1, 2, 3}))
	assert.NoError(t, nf.SetFieldValue(attrs["int64ListCol"], []int64{1, 2, 3}))
	assert.NoError(t, nf.SetFieldValue(attrs["floatListCol"], []float64{1, 2, 3}))
	assert.NoError(t, nf.SetFieldValue(attrs["stringListCol"], []string{"1", "2", "3"}))
	assert.NoError(t, nf.SetFieldValue(attrs["binaryCol"], []byte("foo")))
	date := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.NoError(t, nf.SetFieldValue(attrs["dateCol"], date))
	assert.NoError(t, nf.SetFieldValue(attrs["timeCol"], date.In(time.Local)))
	assert.NoError(t, nf.SetFieldValue(attrs["dateTimeCol"], date.In(calcuttaLoc)))
	// Reload fields from feature to check if they have been properly set
	attrs = nf.Fields()
	sfield := attrs["strCol"]
	assert.True(t, sfield.IsSet())
	assert.Equal(t, FTString, sfield.Type())
	assert.Equal(t, "foo", sfield.String())
	assert.Equal(t, int64(0), sfield.Int())
	assert.Equal(t, 0.0, sfield.Float())
	assert.Nil(t, sfield.IntList())
	assert.Nil(t, sfield.FloatList())
	assert.Nil(t, sfield.StringList())
	assert.Nil(t, sfield.Bytes())
	assert.Nil(t, sfield.DateTime())
	sfield = attrs["intCol"]
	assert.Equal(t, FTInt, sfield.Type())
	assert.Equal(t, "1", sfield.String())
	assert.Equal(t, int64(1), sfield.Int())
	assert.Equal(t, 1.0, sfield.Float())
	sfield = attrs["int64Col"]
	assert.Equal(t, FTInt64, sfield.Type())
	assert.Equal(t, "2", sfield.String())
	assert.Equal(t, int64(2), sfield.Int())
	assert.Equal(t, 2.0, sfield.Float())
	sfield = attrs["floatCol"]
	assert.Equal(t, FTReal, sfield.Type())
	assert.Equal(t, "3.000000", sfield.String())
	assert.Equal(t, int64(3), sfield.Int())
	assert.Equal(t, 3.0, sfield.Float())
	sfield = attrs["intListCol"]
	assert.Equal(t, FTIntList, sfield.Type())
	assert.Equal(t, []int64{1, 2, 3}, sfield.IntList())
	sfield = attrs["int64ListCol"]
	assert.Equal(t, FTInt64List, sfield.Type())
	assert.Equal(t, []int64{1, 2, 3}, sfield.IntList())
	sfield = attrs["floatListCol"]
	assert.Equal(t, FTRealList, sfield.Type())
	assert.Equal(t, []float64{1, 2, 3}, sfield.FloatList())
	sfield = attrs["stringListCol"]
	assert.Equal(t, FTStringList, sfield.Type())
	assert.Equal(t, []string{"1", "2", "3"}, sfield.StringList())
	sfield = attrs["binaryCol"]
	assert.Equal(t, FTBinary, sfield.Type())
	assert.Equal(t, []byte("foo"), sfield.Bytes())
	sfield = attrs["dateCol"]
	assert.Equal(t, FTDate, sfield.Type())
	assert.Equal(t, date.Format(dateFormatRFC3339), sfield.DateTime().Format(dateFormatRFC3339))
	sfield = attrs["timeCol"]
	assert.Equal(t, FTTime, sfield.Type())
	assert.Equal(t, date.In(time.Local).Format(dateFormatRFC3339), sfield.DateTime().Format(dateFormatRFC3339))
	sfield = attrs["dateTimeCol"]
	assert.Equal(t, FTDateTime, sfield.Type())
	assert.Equal(t, date.In(calcuttaLoc).Format(dateFormatRFC3339), sfield.DateTime().Format(dateFormatRFC3339))

	nf, err = lyr.NewFeature(nil)
	assert.NoError(t, err)
	g := nf.Geometry()
	assert.True(t, g.Empty())

	_ = ds.Close()

	/* attempt at raising an error
	RegisterVector(Mitab)
	tmpdir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(tmpdir)
	ds, err = CreateVector(Mitab, filepath.Join(tmpdir, "data.tab"))
	assert.NoError(t, err)
	lyr, err = ds.CreateLayer("l1", nil, GTPoint,
		NewFieldDefinition("strCol", FTString),
	)
	assert.NoError(t, err)
	line, err := NewGeometryFromWKT("LINESTRING (1 1,2 2)", nil)
	assert.NoError(t, err)
	nf, err = lyr.NewFeature(line)
	assert.Error(t, err)
	*/
	ds, _ = CreateVector(Memory, "")
	lyr, err = ds.CreateLayer("l1", nil, GTPoint)
	assert.NoError(t, err)

	pnt, _ = NewGeometryFromWKT("POINT (1 1)", nil)
	nf, _ = lyr.NewFeature(pnt)
	attrs = nf.Fields()
	assert.Len(t, attrs, 0)

	unsupportedFields := []FieldType{FTBinary, FTDate, FTDateTime, FTTime, FTInt64List, FTIntList, FTRealList, FTStringList}
	for _, ft := range unsupportedFields {
		unsupportedField := Field{ftype: ft}
		assert.Equal(t, int64(0), unsupportedField.Int())
		assert.Equal(t, float64(0), unsupportedField.Float())
		assert.Equal(t, "", unsupportedField.String())
	}
}

func TestVSIFile(t *testing.T) {
	fname := "/vsimem/dsakfljhsafdjkl.tif"
	tmpfile := tempfile()
	defer os.Remove(tmpfile)
	ds, _ := Create(GTiff, fname, 1, Byte, 1000, 1000)
	ds.Close()
	ds2, _ := Create(GTiff, tmpfile, 1, Byte, 1000, 1000)
	ds2.Close()

	fbytes, _ := ioutil.ReadFile(tmpfile)

	vf, err := VSIOpen(fname)
	assert.NoError(t, err)
	vf.Close()
	ehc := eh()
	vf, err = VSIOpen(fname, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)

	mbytes, err := ioutil.ReadAll(vf)
	assert.NoError(t, err)

	assert.Equal(t, fbytes, mbytes)

	err = vf.Close()
	assert.NoError(t, err)
	assert.Error(t, vf.Close())

	err = VSIUnlink(fname)
	assert.NoError(t, err)

	_, err = VSIOpen(fname)
	assert.Error(t, err)
	ehc = eh()
	_, err = VSIOpen(fname, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)

	err = VSIUnlink(fname)
	assert.Error(t, err)
	ehc = eh()
	err = VSIUnlink(fname, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
}

func TestUnexpectedVSIAccess(t *testing.T) {
	vpa := vpHandler{datas: make(map[string]KeySizerReaderAt)}
	tifdat, _ := ioutil.ReadFile("testdata/test.tif")
	vpa.datas["test.tif"] = mbufHandler{tifdat}
	err := RegisterVSIHandler("broken://", vpa, VSIHandlerBufferSize(0), VSIHandlerStripPrefix(true))
	assert.NoError(t, err)

	vf, err := VSIOpen("broken://test.tif")
	assert.NoError(t, err)

	l, err := vf.Read(nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, l)
}

type bufHandler []byte
type mbufHandler struct {
	bufHandler
}

func (b bufHandler) ReadAt(_ string, buf []byte, off int64) (int, error) {
	if int(off) >= len(b) {
		return 0, io.EOF
	}
	n := copy(buf, b[off:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}
func (mb mbufHandler) ReadAtMulti(_ string, bufs [][]byte, offs []int64) ([]int, error) {
	ret := make([]int, len(bufs))
	var err error
	for i := range bufs {
		ret[i], err = mb.bufHandler.ReadAt("", bufs[i], offs[i])
		if err != nil {
			return ret, err
		}
	}
	return ret, nil
}
func (b bufHandler) Size(_ string) (int64, error) {
	return int64(len(b)), nil
}

type vpHandler struct {
	datas map[string]KeySizerReaderAt
}
type mvpHandler struct {
	vpHandler
}

func (vp vpHandler) Size(k string) (int64, error) {
	b, ok := vp.datas[k]
	if !ok {
		return 0, syscall.ENOENT
	}
	return b.Size(k)
}

func (vp vpHandler) ReadAt(k string, buf []byte, off int64) (int, error) {
	b, ok := vp.datas[k]
	if !ok {
		return 0, syscall.ENOENT
	}
	return b.ReadAt(k, buf, off)
}

func (mvp mvpHandler) ReadAtMulti(k string, buf [][]byte, off []int64) ([]int, error) {
	b, ok := mvp.datas[k]
	if !ok {
		return nil, syscall.ENOENT
	}
	return b.(KeyMultiReader).ReadAtMulti(k, buf, off)
}

func TestVSIPrefix(t *testing.T) {
	tifdat, _ := ioutil.ReadFile("testdata/test.tif")

	// stripPrefix false
	vpa := vpHandler{datas: make(map[string]KeySizerReaderAt)}
	vpa.datas["prefix://test.tif"] = mbufHandler{tifdat}
	err := RegisterVSIHandler("prefix://", vpa, VSIHandlerStripPrefix(false))
	assert.NoError(t, err)

	ds, err := Open("prefix://test.tif")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	str := ds.Structure()
	if str.SizeX != 10 || str.SizeY != 10 {
		t.Error("wrong structure")
	}
	_, err = Open("prefix://noent")
	if err == nil {
		t.Error("NoEnt not raised")
	}

	// stripPrefix true
	vpa = vpHandler{datas: make(map[string]KeySizerReaderAt)}
	vpa.datas["test.tif"] = mbufHandler{tifdat}

	err = RegisterVSIHandler("noprefix://", vpa, VSIHandlerStripPrefix(true))
	assert.NoError(t, err)

	ds, err = Open("noprefix://test.tif")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	str = ds.Structure()
	if str.SizeX != 10 || str.SizeY != 10 {
		t.Error("wrong structure")
	}
	_, err = Open("noprefix://noent")
	if err == nil {
		t.Error("NoEnt not raised")
	}
}

func TestVSIPlugin(t *testing.T) {
	vpa := vpHandler{datas: make(map[string]KeySizerReaderAt)}
	tifdat, _ := ioutil.ReadFile("testdata/test.tif")
	vpa.datas["test.tif"] = mbufHandler{tifdat}
	err := RegisterVSIHandler("testmem://", vpa, VSIHandlerStripPrefix(true))
	assert.NoError(t, err)
	err = RegisterVSIHandler("testmem://", vpa, VSIHandlerStripPrefix(true))
	assert.Error(t, err)
	ehc := eh()
	err = RegisterVSIHandler("testmem://", vpa, ErrLogger(ehc.ErrorHandler), VSIHandlerStripPrefix(true))
	assert.Error(t, err)
	err = RegisterVSIHandler("/vsimem/", vpa, VSIHandlerStripPrefix(true))
	assert.Error(t, err)

	ds, err := Open("testmem://test.tif")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	str := ds.Structure()
	if str.SizeX != 10 || str.SizeY != 10 {
		t.Error("wrong structure")
	}
	data := make([]byte, 300)
	err = ds.Read(0, 0, data, 10, 10)
	if err != nil {
		t.Error(err)
	}

	_, err = Open("testmem://noent")
	if err == nil {
		t.Error("NoEnt not raised")
	}
}
func TestVSIPluginEx(t *testing.T) {
	vpa := vpHandler{datas: make(map[string]KeySizerReaderAt)}
	tifdat, _ := ioutil.ReadFile("testdata/test.tif")
	vpa.datas["test.tif"] = mbufHandler{tifdat}
	_ = RegisterVSIHandler("testmem2://", vpa, VSIHandlerBufferSize(10), VSIHandlerCacheSize(30), VSIHandlerStripPrefix(true))

	ds, err := Open("testmem2://test.tif")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	str := ds.Structure()
	if str.SizeX != 10 || str.SizeY != 10 {
		t.Error("wrong structure")
	}
	data := make([]byte, 300)
	err = ds.Read(0, 0, data, 10, 10)
	if err != nil {
		t.Error(err)
	}

	_, err = Open("testmem2://noent")
	if err == nil {
		t.Error("NoEnt not raised")
	}
}
func TestVSIPluginNoMulti(t *testing.T) {
	vpa := vpHandler{datas: make(map[string]KeySizerReaderAt)}
	tifdat, _ := ioutil.ReadFile("testdata/test.tif")
	vpa.datas["test.tif"] = bufHandler(tifdat)
	_ = RegisterVSIHandler("testmem3://", vpa, VSIHandlerBufferSize(10), VSIHandlerCacheSize(30), VSIHandlerStripPrefix(true))

	ds, err := Open("testmem3://test.tif")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	str := ds.Structure()
	if str.SizeX != 10 || str.SizeY != 10 {
		t.Error("wrong structure")
	}
	data := make([]byte, 300)
	err = ds.Read(0, 0, data, 10, 10)
	if err != nil {
		t.Error(err)
	}

	_, err = Open("testmem3://noent")
	if err == nil {
		t.Error("NoEnt not raised")
	}
}

type readErroringHandler struct {
	bufHandler
}
type bodyreadErroringHandler struct {
	bufHandler
}
type multireadErroringHandler struct {
	bufHandler
}

func (re readErroringHandler) ReadAt(key string, buf []byte, off int64) (int, error) {
	return 0, fmt.Errorf("not implemented")
}
func (re bodyreadErroringHandler) ReadAt(key string, buf []byte, off int64) (int, error) {
	if off >= 2230 { //2230 is the offset of the strile data in tt.tif
		return 0, fmt.Errorf("read >414 not implemented")
	}
	return re.bufHandler.ReadAt(key, buf, off)
}
func (re multireadErroringHandler) ReadAtMulti(key string, bufs [][]byte, offs []int64) ([]int, error) {
	return nil, fmt.Errorf("mr not implemented")
}

func TestVSIErrors(t *testing.T) {
	tt := tempfile()
	defer os.Remove(tt)
	ds, _ := Create(GTiff, tt, 3, Byte, 2048, 2048, CreationOption("TILED=YES", "COMPRESS=LZW", "BLOCKXSIZE=128", "BLOCKYSIZE=128"))
	ds.Close()
	vpa := vpHandler{datas: make(map[string]KeySizerReaderAt)}
	mvpa := mvpHandler{vpa}
	tifdat, _ := ioutil.ReadFile(tt)
	vpa.datas["test2.tif"] = readErroringHandler{bufHandler(tifdat)}
	vpa.datas["test3.tif"] = multireadErroringHandler{bufHandler(tifdat)}
	vpa.datas["test4.tif"] = bodyreadErroringHandler{bufHandler(tifdat)}
	_ = RegisterVSIHandler("testmem4://", vpa, VSIHandlerBufferSize(0), VSIHandlerCacheSize(0), VSIHandlerStripPrefix(true))
	_ = RegisterVSIHandler("mtestmem4://", mvpa, VSIHandlerBufferSize(0), VSIHandlerCacheSize(0), VSIHandlerStripPrefix(true))

	_, err := Open("testmem4://test2.tif")
	if err == nil {
		t.Error("err not raised")
	}
	data := make([]byte, 300)

	ds, err = Open("mtestmem4://test3.tif")
	if err != nil {
		t.Error(err)
	}

	err = ds.Read(126, 126, data, 10, 10)
	if err == nil {
		t.Error("error not raised")
	}
	ds.Close()

	ds, err = Open("testmem4://test4.tif")
	if err != nil {
		t.Error(err)
	}
	err = ds.Read(126, 126, data, 10, 10)
	if err == nil {
		t.Error("error not raised")
	}
	ds.Close()

	vf, err := VSIOpen("testmem4://test4.tif")
	assert.NoError(t, err)
	_, err = vf.Read(make([]byte, 2230))
	assert.NoError(t, err)
	_, err = vf.Read(make([]byte, 2230))
	assert.EqualError(t, err, "read >414 not implemented")
	n, err := vf.Read(make([]byte, 0, 10))
	assert.Equal(t, 0, n)
	assert.NoError(t, err)

}

func TestBuildVRT(t *testing.T) {
	ds, err := BuildVRT("/vsimem/vrt1.vrt", []string{"testdata/test.tif"}, nil)
	assert.NoError(t, err)
	ds.Close()
	_ = VSIUnlink("/vsimem/vrt1.vrt")

	ehc := eh()
	ds, err = BuildVRT("/vsimem/vrt1.vrt", []string{"testdata/test.tif"}, nil, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	defer func() { _ = VSIUnlink("/vsimem/vrt1.vrt") }()

	str := ds.Structure()
	assert.Equal(t, 10, str.SizeX)
	ds.Close()

	_, err = BuildVRT("/vsimem/vrt1.vrt", []string{"testdata/test.tif"}, nil, DriverOpenOption("BOGUS=GGG"))
	assert.Error(t, err)

	ds, err = BuildVRT("/vsimem/vrt1.vrt", []string{"testdata/test.tif"}, nil, Bands(0), Resampling(Cubic), ConfigOption("VRT_VIRTUAL_OVERVIEWS=YES"))
	assert.NoError(t, err)

	str = ds.Structure()
	assert.Equal(t, 1, str.NBands)
	ds.Close()

	vrtReader, err := VSIOpen("/vsimem/vrt1.vrt")
	assert.NoError(t, err)
	b := bytes.Buffer{}
	_, _ = io.Copy(&b, vrtReader)
	vrtReader.Close()
	assert.Contains(t, b.String(), "resampling=\"cubic\"")
}

func TestVSIGCS(t *testing.T) {
	ctx := context.Background()
	_, err := storage.NewClient(ctx)
	if err != nil {
		t.Skipf("skip test on missing credentials: %v", err)
	}
	gcsh, err := gcs.Handle(ctx)
	if err != nil {
		t.Error(err)
	}
	gcsa, _ := osio.NewAdapter(gcsh)
	err = RegisterVSIHandler("gdalgs://", gcsa, VSIHandlerStripPrefix(true))
	if err != nil {
		t.Error(err)
	}
	ds, err := Open("gdalgs://godal-ci-data/test.tif")
	if err != nil {
		t.Error(err)
		return
	}
	defer ds.Close()
	if ds.Structure().SizeX != 10 {
		t.Errorf("xsize: %d", ds.Structure().SizeX)
	}
	data := make([]byte, 300)
	err = ds.Read(0, 0, data, 10, 10)
	if err != nil {
		t.Error(err)
	}

	for _, tc := range []string{
		"gdalgs",
		"gdalgs:",
		"gdalgs:/",
		"gdalgs://",
		"gdalgs://godal-ci-data",
		"gdalgs://godal-ci-data/",
		"gdalgs://godal-ci-data/gdd/doesnotexist.tif",
		"gdalgs://godal-fake-test/gdaltesdata/doesnotexist.tif",
	} {
		_, err = Open(tc)
		assert.Error(t, err, "ENONENT not raised on %s", tc)
	}
}

func TestVSIGCSNoAuth(t *testing.T) {
	ctx := context.Background()
	st, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		t.Skipf("failed to create gcs client: %v", err)
	}
	gcsh, _ := gcs.Handle(ctx, gcs.GCSClient(st))
	gcsa, _ := osio.NewAdapter(gcsh)
	err = RegisterVSIHandler("gdalgcs://", gcsa, VSIHandlerStripPrefix(true))
	if err != nil {
		t.Error(err)
	}
	ds, err := Open("gdalgcs://godal-ci-data/test.tif")
	if err != nil {
		t.Error(err)
		return
	}
	defer ds.Close()
	if ds.Structure().SizeX != 10 {
		t.Errorf("xsize: %d", ds.Structure().SizeX)
	}
	data := make([]byte, 300)
	err = ds.Read(0, 0, data, 10, 10)
	if err != nil {
		t.Error(err)
	}

	_, err = Open("gdalgcs://godal-ci-data/gdd/doesnotexist.tif")
	if err == nil {
		t.Error("ENOENT not raised")
	}
	_, err = Open("gdalgs://godal-fake-test/gdaltesdata/doesnotexist.tif")
	if err == nil {
		t.Error("ENOENT not raised")
	}
}

type errLogger struct {
	msg    []string
	thresh ErrorCategory
}

// this is an example error handler that returns an error if its level is over thresh,
// or logs the message in its msg []string if under
func (e *errLogger) ErrorHandler(ec ErrorCategory, code int, message string) error {
	if ec >= e.thresh {
		return errors.New(message)
	}
	e.msg = append(e.msg, message)
	return nil
}

func TestErrorHandling(t *testing.T) {
	err := testErrorAndLogging()
	assert.EqualError(t, err, "this is a warning message\nthis is a failure message")

	el := errLogger{thresh: CE_Warning}
	err = testErrorAndLogging(ErrLogger(el.ErrorHandler))
	assert.EqualError(t, err, "this is a warning message\nthis is a failure message")

	el.thresh = CE_Fatal
	el.msg = nil
	err = testErrorAndLogging(ErrLogger(el.ErrorHandler))
	assert.NoError(t, err)

	el.thresh = CE_Failure
	el.msg = nil
	err = testErrorAndLogging(ErrLogger(el.ErrorHandler), ConfigOption("CPL_DEBUG=ON"))
	assert.EqualError(t, err, "this is a failure message")
	assert.Equal(t, []string{
		"godal: this is a debug message",
		"this is a warning message",
	}, el.msg)

	el.msg = nil
	err = testErrorAndLogging(ErrLogger(el.ErrorHandler))
	assert.EqualError(t, err, "this is a failure message")
	assert.Equal(t, []string{"this is a warning message"}, el.msg)

	//warning message should not show up
	err = testErrorAndLogging(SkipWarnings)
	assert.EqualError(t, err, "this is a failure message")
}

type debugLogger struct {
	logs string
}

func (dl *debugLogger) L(ec ErrorCategory, code int, msg string) error {
	if ec >= CE_Warning {
		return fmt.Errorf(msg)
	}
	if ec == CE_Debug {
		dl.logs += ",GOTESTDEBUG:" + msg
	}
	return nil
}
func (dl *debugLogger) reset() {
	dl.logs = ""
}

func TestConfigOptionsExtended(t *testing.T) {
	dl := &debugLogger{}

	ds, _ := Open("testdata/test.tif")
	defer ds.Close()
	dsm, _ := ds.Translate("", nil, Memory)
	defer dsm.Close()
	ds2, _ := Open("testdata/test.tif")
	defer ds2.Close()

	dl.reset()
	err := dsm.WarpInto([]*Dataset{ds2}, nil, ErrLogger(dl.L), ConfigOption("CPL_DEBUG=ON"))
	assert.NoError(t, err)
	assert.Contains(t, dl.logs, "GOTESTDEBUG:") //contains something like "GDALWARP: Defining SKIP_NOSOURCE=YES,WARP: Copying metadata from first source to destination dataset,GDAL: Computing area of interest: 45, 25, 55, 35,OGRCT: Wrap source at 50.,GTiff: ScanDirectories(),GDAL: GDALDefaultOverviews::OverviewScan(),WARP: band=0 dstNoData=99.000000,WARP: band=1 dstNoData=99.000000,WARP: band=2 dstNoData=99.000000,GDAL: GDALWarpKernel()::GWKNearestByte() Src=0,0,10x10 Dst=0,0,10x10"
	buf := make([]byte, 3)

	//force 0 pixel read to emit a dbug message
	dl.reset()
	err = ds.Read(0, 0, buf, 0, 1, ErrLogger(dl.L), ConfigOption("CPL_DEBUG=ON"))
	assert.NoError(t, err)
	assert.Contains(t, dl.logs, "GOTESTDEBUG:")

	//force 0 pixel read to emit a dbug message
	dl.reset()
	err = ds.Bands()[0].Read(0, 0, buf, 0, 1, ErrLogger(dl.L), ConfigOption("CPL_DEBUG=ON"))
	assert.NoError(t, err)
	assert.Contains(t, dl.logs, "GOTESTDEBUG:")

}

type custErr struct {
	msg string
}

func (e *custErr) Error() string {
	return e.msg
}

type custErr2 struct {
	msg string
}

func (e *custErr2) Error() string {
	return e.msg
}

type custErr3 struct {
	msg string
}

func (e *custErr3) Error() string {
	return e.msg
}

func TestMultiError(t *testing.T) {
	e1 := &custErr{"e1"}
	e2 := &custErr2{"e2"}
	e3 := &custErr3{"e3"}

	var cerr *custErr
	var cerr2 *custErr2
	var cerr3 *custErr3
	e11 := combine(nil, e1)
	assert.True(t, errors.Is(e11, e1))
	assert.True(t, errors.As(e11, &cerr))
	assert.False(t, errors.As(e11, &cerr2))
	assert.Equal(t, "e1", cerr.msg)

	e11 = combine(e1, nil)
	assert.True(t, errors.Is(e11, e1))
	assert.True(t, errors.As(e11, &cerr))
	assert.False(t, errors.As(e11, &cerr2))
	assert.Equal(t, "e1", cerr.msg)

	e12 := combine(e1, e2)
	assert.True(t, errors.Is(e12, e1))
	assert.True(t, errors.Is(e12, e2))
	assert.False(t, errors.Is(e12, e3))
	assert.True(t, errors.As(e12, &cerr))
	assert.True(t, errors.As(e12, &cerr2))
	assert.False(t, errors.As(e12, &cerr3))
	assert.Equal(t, "e1", cerr.msg)
	assert.Equal(t, "e2", cerr2.msg)

	e123 := combine(e12, e3)
	assert.True(t, errors.Is(e123, e1))
	assert.True(t, errors.Is(e123, e2))
	assert.True(t, errors.Is(e123, e3))

	e312 := combine(e3, e12)
	assert.True(t, errors.Is(e312, e1))
	assert.True(t, errors.Is(e312, e2))
	assert.True(t, errors.Is(e312, e3))

	e12 = combine(e1, e2)
	e13 := combine(e1, e3)
	e1213 := combine(e12, e13)
	assert.True(t, errors.Is(e1213, e1))
	assert.True(t, errors.Is(e1213, e2))
	assert.True(t, errors.Is(e1213, e3))
}

func TestSieveFilter(t *testing.T) {
	ds, _ := Create(Memory, "", 1, Byte, 10, 10)
	dsb := ds.Bands()[0]
	ds2, _ := Create(Memory, "", 1, Byte, 10, 10)
	dsb2 := ds2.Bands()[0]
	defer ds.Close()
	defer ds2.Close()
	_ = dsb.SetNoData(0)

	buf := make([]byte, 100)
	reset := func(val byte) {
		for i := range buf {
			buf[i] = val
		}
	}

	// using only nodata
	reset(2)
	buf[11] = 0
	buf[12] = 1
	_ = dsb.Write(0, 0, buf, 10, 10)
	err := dsb.SieveFilter(3)
	assert.NoError(t, err)
	_ = dsb.Read(0, 0, buf, 10, 10)
	assert.Equal(t, byte(0), buf[11]) //check nodata preserved
	assert.Equal(t, byte(2), buf[12]) //check sieve modified pixel

	// using explicit mask band
	reset(2)
	buf[13] = 0
	_ = dsb2.Write(0, 0, buf, 10, 10) //buf2 is nodata mask
	reset(2)
	buf[12] = 1
	buf[13] = 1
	_ = dsb.Write(0, 0, buf, 10, 10)
	err = dsb.SieveFilter(3, Mask(dsb2))
	assert.NoError(t, err)
	_ = dsb.Read(0, 0, buf, 10, 10)
	assert.Equal(t, byte(1), buf[13]) //check nodatamask preserved
	assert.Equal(t, byte(2), buf[12]) //check sieve modified pixel

	//ignore nodata mask
	reset(2)
	buf[11] = 0
	buf[12] = 0
	_ = dsb.Write(0, 0, buf, 10, 10)
	err = dsb.SieveFilter(3, NoMask())
	assert.NoError(t, err)
	_ = dsb.Read(0, 0, buf, 10, 10)
	assert.Equal(t, byte(2), buf[11]) //check nodata ignored
	assert.Equal(t, byte(2), buf[12]) //check sieve modified pixel

	//test eight connectedness
	reset(2)
	for i := 0; i < 10; i++ {
		buf[i*10+i] = 1 //diagonal
	}
	_ = dsb.Write(0, 0, buf, 10, 10)
	err = dsb.SieveFilter(3, EightConnected())
	assert.NoError(t, err)
	_ = dsb.Read(0, 0, buf, 10, 10)
	assert.Equal(t, byte(1), buf[0]) //check not sieved as on a 8-connected polygon of 10 pixels

	//test destination band
	reset(3)
	_ = dsb2.Write(0, 0, buf, 10, 10)
	//dsb is still the diagonal
	err = dsb.SieveFilter(3, Destination(dsb2))
	assert.NoError(t, err)
	_ = dsb.Read(0, 0, buf, 10, 10)
	assert.Equal(t, byte(1), buf[0]) //check not modified in source band
	assert.Equal(t, byte(2), buf[1]) //check not modified in source band
	_ = dsb2.Read(0, 0, buf, 10, 10)
	assert.Equal(t, byte(2), buf[1]) //check copied to destination band
	assert.Equal(t, byte(2), buf[0]) //check modified in destination band

	// test error handling
	err = Band{}.SieveFilter(3)
	assert.Error(t, err)
	ehc := eh()
	err = Band{}.SieveFilter(3, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	assert.Equal(t, 1, ehc.errs)
}

func TestStatistics(t *testing.T) {
	pix := []float64{-1, -1, -1, 0.23, 4.04, 3.96, 1.8, 2.5, 1.31, 0.8, 0.12,
		3.43, 0.23, 3.31, 3.19, 2.09, 3.25, 3.21, 1.04, 2.3, 3.83, 0.97,
		0.69, -1, -1}
	ds, _ := Create(Memory, "", 1, Float64, 5, 5)
	defer ds.Close()
	_ = ds.Write(0, 0, pix, 5, 5)
	bnd := ds.Bands()[0]
	_ = bnd.SetNoData(-1)
	// Test Empty statistics
	stats, flag, err := bnd.GetStatistics()
	assert.NoError(t, err)
	assert.Equal(t, false, flag)
	assert.Equal(t, 0., stats.Min)
	assert.Equal(t, 0., stats.Max)
	assert.Equal(t, 0., stats.Mean)
	assert.Equal(t, 0., stats.Std)
	assert.Equal(t, false, stats.Approximate)
	// Test Pre computed statistics
	min := 5.
	max := 0.94
	mean := 10.
	std := 0.29
	ehc := eh()
	err = bnd.SetStatistics(min, max, mean, std, ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	stats, flag, err = bnd.GetStatistics()
	assert.NoError(t, err)
	assert.Equal(t, true, flag)
	assert.Equal(t, 5., stats.Min)
	assert.Equal(t, 0.94, stats.Max)
	assert.Equal(t, 10., stats.Mean)
	assert.Equal(t, 0.29, stats.Std)
	assert.Equal(t, false, stats.Approximate)
	runtimeVersion := Version()
	err = ds.ClearStatistics()
	if runtimeVersion.Major() <= 3 && runtimeVersion.Minor() < 2 {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
	}
	ehc = eh()
	err = ds.ClearStatistics(ErrLogger(ehc.ErrorHandler))
	if runtimeVersion.Major() <= 3 && runtimeVersion.Minor() < 2 {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
	}
	// Test exact computed statistics
	ehc = eh()
	stats, err = bnd.ComputeStatistics(ErrLogger(ehc.ErrorHandler))
	assert.NoError(t, err)
	assert.Equal(t, 0.12, stats.Min)
	assert.Equal(t, 4.04, stats.Max)
	assert.Equal(t, 2.12, math.Ceil(stats.Mean*100)/100)
	assert.Equal(t, 1.32, math.Round(stats.Std*100)/100)
	assert.Equal(t, false, stats.Approximate)
	stats, flag, err = bnd.GetStatistics()
	assert.NoError(t, err)
	assert.Equal(t, true, flag)
	assert.Equal(t, 0.12, stats.Min)
	assert.Equal(t, 4.04, stats.Max)
	assert.Equal(t, 2.12, math.Round(stats.Mean*100)/100)
	assert.Equal(t, 1.32, math.Round(stats.Std*100)/100)
	assert.Equal(t, false, stats.Approximate)
	exact_stats := stats
	_ = ds.ClearStatistics()
	// Test approximated computed statistics
	stats, err = bnd.ComputeStatistics(Approximate())
	assert.NoError(t, err)
	assert.Equal(t, exact_stats.Min, stats.Min)
	assert.Equal(t, exact_stats.Max, stats.Max)
	assert.NotEqual(t, exact_stats.Mean, stats.Mean)
	assert.NotEqual(t, exact_stats.Std, stats.Std)
	assert.Equal(t, true, stats.Approximate)
	// Test whether there is no Approximated stats
	stats, flag, err = bnd.GetStatistics()
	assert.NoError(t, err)
	assert.Equal(t, false, flag)
	// Test whether there is Approximated stats
	stats, flag, err = bnd.GetStatistics(Approximate())
	assert.NoError(t, err)
	assert.Equal(t, true, flag)
	assert.Equal(t, 0.12, stats.Min)
	assert.Equal(t, 4.04, stats.Max)
	assert.Equal(t, 2., math.Round(stats.Mean*100)/100)
	assert.Equal(t, 1.59, math.Round(stats.Std*100)/100)
	// Test with full no data for coverage
	pix = []float64{1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1., 1.,
		1., 1., 1., 1., 1., 1., 1., 1.}
	_ = ds.ClearStatistics()
	_ = ds.Write(0, 0, pix, 5, 5)
	bnd = ds.Bands()[0]
	_ = bnd.SetNoData(1)
	stats, err = bnd.ComputeStatistics()
	assert.Error(t, err)
	assert.Equal(t, 0., stats.Min)
	assert.Equal(t, 0., stats.Max)
	assert.Equal(t, 0., stats.Mean)
	assert.Equal(t, 0., stats.Std)
	assert.Equal(t, false, stats.Approximate)
	_ = ds.ClearStatistics()
	// Test null band for coverage
	bnd = Band{}
	err = bnd.SetStatistics(min, max, mean, std)
	assert.Error(t, err)
	// Test on null band for coverage
	_, _, err = bnd.GetStatistics()
	assert.Error(t, err)
}

func TestGridLinear(t *testing.T) {
	var (
		err      error
		outXSize = 256
		outYSize = 256
	)

	vrtDs, err := CreateVector(Memory, "")
	if err != nil {
		t.Error(err)
		return
	}

	geom, err := NewGeometryFromWKT("POLYGON((0 0 0, 0 1 1, 1 1 0, 1 0 1))", nil)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = vrtDs.CreateLayer("grid", nil, GTPolygon)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = vrtDs.Layers()[0].NewFeature(geom)
	if err != nil {
		t.Error(err)
		return
	}

	// As of GDAL v3.6, `GDALGrid` will swap `yMin` and `yMax` if `yMin` < `yMax`. In order to make the output of
	// earlier GDAL versions (< 3.6) consistent with this, we're setting `yMin` > `yMax`.
	yMin := 1
	yMax := 0
	argsString := fmt.Sprintf("-a linear -txe 0 1 -tye %d %d -outsize %d %d -ot Float64", yMin, yMax, outXSize, outYSize)
	fname := "/vsimem/test.tiff"

	gridDs, err := vrtDs.Grid(fname, strings.Split(argsString, " "))
	if err != nil {
		// Handles QHull error differently here, as it's a compatibility issue not a gridding error
		isQhullError := strings.HasSuffix(err.Error(), "without QHull support")
		if isQhullError {
			t.Log(`Skipping test, GDAL was built without "Delaunay triangulation" support which is required for the "Linear" gridding algorithm`)
			return
		} else {
			t.Error(err)
			return
		}
	}
	defer func() { _ = VSIUnlink(fname) }()
	defer gridDs.Close()

	var gridBindingPoints = make([]float64, outXSize*outYSize)
	err = gridDs.Read(0, 0, gridBindingPoints, outXSize, outYSize)
	if err != nil {
		t.Error(err)
		return
	}

	var (
		topLeftIndex     = 0
		topRightIndex    = outXSize - 1
		bottomLeftIndex  = outXSize * (outYSize - 1)
		bottomRightIndex = (outXSize * outYSize) - 1
		imageCentreIndex = outYSize*(outXSize/2) - 1
	)

	// For linear interpolation, we expect z-values of corners to match the input coordinates
	// and the centre value to be the average of the 4 corner values
	// 	TL (0, 0, 0), EXPECTED OUTPUT Z-VAL  = 0
	// 	TR (1, 0, 1), EXPECTED OUTPUT Z-VAL  = 1
	// 	BL (0, 1, 0), EXPECTED OUTPUT Z-VAL  = 0
	// 	BR (1, 1, 1), EXPECTED OUTPUT Z-VAL  = 1
	//  CR (0.5, 0.5), EXPECTED OUTPUT Z-VAL = (0 + 1 + 0 + 1) / 4 = 0.5
	//
	// NOTE: The input X and Y coords are offset slightly in GDAL before they're passed into a a gridding algorithm.
	// This is why "TR" and "BL" below are expected to be 0.00390625 and NOT 0.
	// See the `dfXPoint` and `dfYPoint` values in `GDALGridJobProcess()` for how these points are calculated
	// TL
	assert.Equal(t, 1.0, gridBindingPoints[topLeftIndex])
	// TR
	assert.Equal(t, 0.00390625, gridBindingPoints[topRightIndex])
	// BL
	assert.Equal(t, 0.00390625, gridBindingPoints[bottomLeftIndex])
	// BR
	assert.Equal(t, 1.0, gridBindingPoints[bottomRightIndex])
	// Center
	assert.Equal(t, 0.5, gridBindingPoints[imageCentreIndex])
}

func TestGridCreateLinear(t *testing.T) {
	var (
		err error

		xCoords  = []float64{0, 1, 0, 1}
		yCoords  = []float64{0, 0, 1, 1}
		zCoords  = []float64{1, 0, 0, 1}
		outXSize = 256
		outYSize = 256
	)

	var gridCreateBindingPoints = make([]float64, outXSize*outYSize)
	err = GridCreate("linear", xCoords, yCoords, zCoords, 0, 1, 0, 1, outXSize, outYSize, gridCreateBindingPoints)
	if err != nil {
		// Handles QHull error differently here, as it's a compatibility issue not a gridding error
		isQhullError := strings.HasSuffix(err.Error(), "without QHull support")
		if isQhullError {
			t.Log(`Skipping test, GDAL was built without "Delaunay triangulation" support which is required for the "Linear" gridding algorithm`)
			return
		} else {
			t.Error(err)
			return
		}
	}

	var (
		topLeftIndex     = 0
		topRightIndex    = outXSize - 1
		bottomLeftIndex  = outXSize * (outYSize - 1)
		bottomRightIndex = (outXSize * outYSize) - 1
		imageCentreIndex = outYSize*(outXSize/2) - 1
	)

	// For linear interpolation, we expect z-values of corners to match the input coordinates
	// and the centre value to be the average of the 4 corner values
	// 	TL (0, 0, 0), EXPECTED OUTPUT Z-VAL  = 0
	// 	TR (1, 0, 1), EXPECTED OUTPUT Z-VAL  = 1
	// 	BL (0, 1, 0), EXPECTED OUTPUT Z-VAL  = 0
	// 	BR (1, 1, 1), EXPECTED OUTPUT Z-VAL  = 1
	//  CR (0.5, 0.5), EXPECTED OUTPUT Z-VAL = (0 + 1 + 0 + 1) / 4 = 0.5
	//
	// NOTE: The input X and Y coords are offset slightly in GDAL before they're passed into a a gridding algorithm.
	// This is why "TR" and "BL" below are expected to be 0.00390625 and NOT 0.
	// See the `dfXPoint` and `dfYPoint` values in `GDALGridJobProcess()` for how these points are calculated
	// TL
	assert.Equal(t, 1.0, gridCreateBindingPoints[topLeftIndex])
	// TR
	assert.Equal(t, 0.00390625, gridCreateBindingPoints[topRightIndex])
	// BL
	assert.Equal(t, 0.00390625, gridCreateBindingPoints[bottomLeftIndex])
	// BR
	assert.Equal(t, 1.0, gridCreateBindingPoints[bottomRightIndex])
	// Center
	assert.Equal(t, 0.5, gridCreateBindingPoints[imageCentreIndex])
}

func TestGridMaximum(t *testing.T) {
	var (
		err      error
		outXSize = 256
		outYSize = 256
	)

	vrtDs, err := CreateVector(Memory, "")
	if err != nil {
		t.Error(err)
		return
	}

	geom, err := NewGeometryFromWKT("POLYGON((0 0 0, 0 1 1, 1 1 0, 1 0 1))", nil)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = vrtDs.CreateLayer("grid", nil, GTPolygon)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = vrtDs.Layers()[0].NewFeature(geom)
	if err != nil {
		t.Error(err)
		return
	}

	// NOTE: Flipping the arguments after `-tye` here, to account for `ProcessLayer` (in `GDALGrid`) flipping the coords "north up"
	argsString := fmt.Sprintf("-a maximum -txe 0 1 -tye 0 1 -outsize %d %d -ot Float64", outXSize, outYSize)
	fname := "/vsimem/test.tiff"

	gridDs, err := vrtDs.Grid(fname, strings.Split(argsString, " "))
	if err != nil {
		t.Error(err)
		return
	}
	defer func() { _ = VSIUnlink(fname) }()
	defer gridDs.Close()

	var gridBindingPoints = make([]float64, outXSize*outYSize)
	err = gridDs.Read(0, 0, gridBindingPoints, outXSize, outYSize)
	if err != nil {
		t.Error(err)
		return
	}

	var (
		topLeftIndex     = 0
		topRightIndex    = outXSize - 1
		bottomLeftIndex  = outXSize * (outYSize - 1)
		bottomRightIndex = (outXSize * outYSize) - 1
		imageCentreIndex = outYSize*(outXSize/2) - 1
	)

	// All sampled values are expected to match the "maximum" value in the grid i.e. 1
	// TL
	assert.Equal(t, 1.0, gridBindingPoints[topLeftIndex])
	// TR
	assert.Equal(t, 1.0, gridBindingPoints[topRightIndex])
	// BL
	assert.Equal(t, 1.0, gridBindingPoints[bottomLeftIndex])
	// BR
	assert.Equal(t, 1.0, gridBindingPoints[bottomRightIndex])
	// Center
	assert.Equal(t, 1.0, gridBindingPoints[imageCentreIndex])
}

func TestGridCreateMaximum(t *testing.T) {
	var (
		err error

		xCoords  = []float64{0, 1, 0, 1}
		yCoords  = []float64{0, 0, 1, 1}
		zCoords  = []float64{1, 0, 0, 1}
		outXSize = 256
		outYSize = 256
	)

	var gridCreateBindingPoints = make([]float64, outXSize*outYSize)
	err = GridCreate("maximum", xCoords, yCoords, zCoords, 0, 1, 0, 1, outXSize, outYSize, gridCreateBindingPoints)
	if err != nil {
		t.Error(err)
		return
	}

	var (
		topLeftIndex     = 0
		topRightIndex    = outXSize - 1
		bottomLeftIndex  = outXSize * (outYSize - 1)
		bottomRightIndex = (outXSize * outYSize) - 1
		imageCentreIndex = outYSize*(outXSize/2) - 1
	)
	// All sampled values are expected to match the "maximum" value in the grid i.e. 1
	// TL
	assert.Equal(t, 1.0, gridCreateBindingPoints[topLeftIndex])
	// TR
	assert.Equal(t, 1.0, gridCreateBindingPoints[topRightIndex])
	// BL
	assert.Equal(t, 1.0, gridCreateBindingPoints[bottomLeftIndex])
	// BR
	assert.Equal(t, 1.0, gridCreateBindingPoints[bottomRightIndex])
	// Center
	assert.Equal(t, 1.0, gridCreateBindingPoints[imageCentreIndex])
}

func TestGridUnknownSwitch(t *testing.T) {
	vrtDs, err := CreateVector(Memory, "")
	if err != nil {
		t.Error(err)
		return
	}

	geom, err := NewGeometryFromWKT("POLYGON((0 0 0, 0 1 1, 1 1 0, 1 0 1))", nil)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = vrtDs.CreateLayer("grid", nil, GTPolygon)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = vrtDs.Layers()[0].NewFeature(geom)
	if err != nil {
		t.Error(err)
		return
	}

	ehc := eh()
	_, err = vrtDs.Grid("/vsimem/test.tiff", []string{"-invalidswitch"}, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
}
