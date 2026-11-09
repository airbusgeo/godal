// Copyright 2026 Airbus Defence and Space
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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMemMultiDim creates an in-memory (time=2, lat=3, lon=4) float32 array
// named "t2m" whose value at (t,y,x) is t*100+y*10+x
func createMemMultiDim(t *testing.T) (*Dataset, *Group, *MDArray) {
	t.Helper()
	ds, err := CreateMultiDimensional(Memory, "")
	require.NoError(t, err)
	root, err := ds.RootGroup()
	require.NoError(t, err)
	require.NoError(t, root.CreateDimension(Dimension{Name: "time", Type: "TEMPORAL", Size: 2}))
	require.NoError(t, root.CreateDimension(Dimension{Name: "lat", Type: "HORIZONTAL_Y", Direction: "NORTH", Size: 3}))
	require.NoError(t, root.CreateDimension(Dimension{Name: "lon", Type: "HORIZONTAL_X", Direction: "EAST", Size: 4}))
	arr, err := root.CreateMDArray("t2m", []string{"time", "lat", "lon"}, Float32)
	require.NoError(t, err)
	buf := make([]float32, 2*3*4)
	for i := range buf {
		buf[i] = float32((i/12)*100 + ((i/4)%3)*10 + i%4)
	}
	require.NoError(t, arr.Write([]int{0, 0, 0}, []int{2, 3, 4}, buf))
	return ds, root, arr
}

func TestMultiDimCreateReadWrite(t *testing.T) {
	ds, root, arr := createMemMultiDim(t)
	defer ds.Close()
	defer root.Close()
	defer arr.Close()

	assert.Equal(t, "/", root.Name())
	assert.Equal(t, "/", root.FullName())
	assert.Equal(t, "t2m", arr.Name())
	assert.Equal(t, "/t2m", arr.FullName())
	assert.Equal(t, Float32, arr.DataType())

	names, err := root.MDArrayNames()
	assert.NoError(t, err)
	assert.Equal(t, []string{"t2m"}, names)
	gnames, err := root.GroupNames()
	assert.NoError(t, err)
	assert.Equal(t, []string{}, gnames)

	dims, err := arr.Dimensions()
	require.NoError(t, err)
	require.Len(t, dims, 3)
	assert.Equal(t, Dimension{Name: "time", Type: "TEMPORAL", Size: 2}, dims[0])
	assert.Equal(t, Dimension{Name: "lat", Type: "HORIZONTAL_Y", Direction: "NORTH", Size: 3}, dims[1])
	assert.Equal(t, Dimension{Name: "lon", Type: "HORIZONTAL_X", Direction: "EAST", Size: 4}, dims[2])
	shape, err := arr.Shape()
	assert.NoError(t, err)
	assert.Equal(t, []uint64{2, 3, 4}, shape)
	gdims, err := root.Dimensions()
	assert.NoError(t, err)
	assert.Len(t, gdims, 3)

	// read a sub-window: time=1, lat=1..2, lon=2..3
	sub := make([]float32, 4)
	require.NoError(t, arr.Read([]int{1, 1, 2}, []int{1, 2, 2}, sub))
	assert.Equal(t, []float32{112, 113, 122, 123}, sub)

	// read with type conversion
	sub64 := make([]float64, 4)
	require.NoError(t, arr.Read([]int{0, 2, 0}, []int{1, 1, 4}, sub64))
	assert.Equal(t, []float64{20, 21, 22, 23}, sub64)
	sub16 := make([]int16, 2)
	require.NoError(t, arr.Read([]int{0, 0, 0}, []int{2, 1, 1}, sub16))
	assert.Equal(t, []int16{0, 100}, sub16)

	// strided reads: every other lon, and walking lat backwards
	strided := make([]float32, 2)
	require.NoError(t, arr.Read([]int{0, 0, 0}, []int{1, 1, 2}, strided, Steps(1, 1, 2)))
	assert.Equal(t, []float32{0, 2}, strided)
	rev := make([]float32, 3)
	require.NoError(t, arr.Read([]int{0, 2, 1}, []int{1, 3, 1}, rev, Steps(1, -1, 1)))
	assert.Equal(t, []float32{21, 11, 1}, rev)

	// partial write
	require.NoError(t, arr.Write([]int{0, 0, 0}, []int{1, 1, 2}, []float32{-1, -2}))
	require.NoError(t, arr.Read([]int{0, 0, 0}, []int{1, 1, 3}, rev))
	assert.Equal(t, []float32{-1, -2, 2}, rev)

	// zero-sized reads are a no-op
	assert.NoError(t, arr.Read([]int{0, 0, 0}, []int{0, 1, 1}, []float32{}))

	// metadata
	_, ok := arr.NoData()
	assert.False(t, ok)
	require.NoError(t, arr.SetNoData(-9999))
	nd, ok := arr.NoData()
	assert.True(t, ok)
	assert.Equal(t, -9999.0, nd)

	scale, offset := arr.ScaleOffset()
	assert.Equal(t, 1.0, scale)
	assert.Equal(t, 0.0, offset)
	require.NoError(t, arr.SetScaleOffset(0.5, 273.15))
	scale, offset = arr.ScaleOffset()
	assert.Equal(t, 0.5, scale)
	assert.Equal(t, 273.15, offset)

	assert.Equal(t, "", arr.Unit())
	require.NoError(t, arr.SetUnit("K"))
	assert.Equal(t, "K", arr.Unit())

	assert.Nil(t, arr.SpatialRef())

	// attributes
	require.NoError(t, arr.SetAttribute("long_name", "2 metre temperature"))
	require.NoError(t, arr.SetAttribute("valid_min", 180.0))
	require.NoError(t, arr.SetAttribute("valid_range", []float64{180, 340}))
	attrs, err := arr.Attributes()
	require.NoError(t, err)
	require.Len(t, attrs, 3)
	assert.Equal(t, "long_name", attrs[0].Name)
	assert.True(t, attrs[0].IsString())
	assert.Equal(t, []string{"2 metre temperature"}, attrs[0].Strings)
	assert.Equal(t, Unknown, attrs[0].DataType)
	assert.Equal(t, "2 metre temperature", attrs[0].String())
	assert.Equal(t, "valid_min", attrs[1].Name)
	assert.False(t, attrs[1].IsString())
	assert.Equal(t, Float64, attrs[1].DataType)
	assert.Nil(t, attrs[1].Shape)
	assert.Equal(t, []float64{180}, attrs[1].Values)
	assert.Equal(t, "180", attrs[1].String())
	assert.Equal(t, []uint64{2}, attrs[2].Shape)
	assert.Equal(t, []float64{180, 340}, attrs[2].Values)
	assert.Equal(t, "[180 340]", attrs[2].String())

	require.NoError(t, root.SetAttribute("Conventions", "CF-1.8"))
	gattrs, err := root.Attributes()
	require.NoError(t, err)
	require.Len(t, gattrs, 1)
	assert.Equal(t, "CF-1.8", gattrs[0].String())

	err = arr.SetAttribute("bad", 3)
	assert.Error(t, err)
	err = arr.SetAttribute("empty", []float64{})
	assert.Error(t, err)

	// subgroups
	sg, err := root.CreateGroup("surface", ConfigOption("CPL_DEBUG=OFF"))
	require.NoError(t, err)
	_, err = root.CreateGroup("surface", ErrLogger(func(ec ErrorCategory, code int, msg string) error { return nil }))
	assert.Error(t, err)
	assert.Equal(t, "surface", sg.Name())
	assert.Equal(t, "/surface", sg.FullName())
	require.NoError(t, sg.CreateDimension(Dimension{Name: "level", Size: 5}))
	larr, err := sg.CreateMDArray("pressure", []string{"level"}, Float64)
	require.NoError(t, err)
	require.NoError(t, larr.Write([]int{0}, []int{5}, []float64{1000, 925, 850, 700, 500}))
	larr.Close()
	larr.Close() // closing twice is a noop
	sg.Close()
	sg.Close()

	gnames, err = root.GroupNames()
	assert.NoError(t, err)
	assert.Equal(t, []string{"surface"}, gnames)
	sg, err = root.OpenGroup("surface")
	require.NoError(t, err)
	defer sg.Close()
	larr, err = sg.OpenMDArray("pressure")
	require.NoError(t, err)
	defer larr.Close()
	assert.Equal(t, "/surface/pressure", larr.FullName())
	lv := make([]float64, 2)
	require.NoError(t, larr.Read([]int{3}, []int{2}, lv))
	assert.Equal(t, []float64{700, 500}, lv)
}

func TestMultiDimErrors(t *testing.T) {
	ds, root, arr := createMemMultiDim(t)
	defer ds.Close()
	defer root.Close()
	defer arr.Close()

	_, err := root.OpenMDArray("nope")
	assert.Error(t, err)
	_, err = root.OpenGroup("nope")
	assert.Error(t, err)
	_, err = root.CreateMDArray("bad", []string{"time", "nope"}, Float32)
	assert.Error(t, err)

	buf := make([]float32, 24)
	assert.Error(t, arr.Read([]int{0, 0}, []int{2, 3, 4}, buf))
	assert.Error(t, arr.Read([]int{0, 0, 0}, []int{2, 3, 4}, buf, Steps(1, 1)))
	assert.Error(t, arr.Read([]int{-1, 0, 0}, []int{1, 1, 1}, buf))
	// out of bounds
	assert.Error(t, arr.Read([]int{0, 0, 3}, []int{1, 1, 2}, buf))
	assert.Panics(t, func() { _ = arr.Read([]int{0, 0, 0}, []int{2, 3, 4}, make([]float32, 23)) })

	// errors are routed to the ErrLogger handler
	ehc := eh()
	err = arr.Read([]int{0, 0, 3}, []int{1, 1, 2}, buf, ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	assert.NotZero(t, ehc.errs)
	ehc = eh()
	_, err = root.OpenMDArray("nope", ErrLogger(ehc.ErrorHandler))
	assert.Error(t, err)
	assert.NotZero(t, ehc.errs)
	// an error handler that ignores errors still yields an error for a missing array
	_, err = root.OpenMDArray("nope", ErrLogger(func(ec ErrorCategory, code int, msg string) error { return nil }))
	assert.Error(t, err)

	_, err = arr.View("[0,", ConfigOption("CPL_DEBUG=OFF"))
	assert.Error(t, err)
	_, err = arr.AsClassicDataset(5, 0)
	assert.Error(t, err)
	_, err = arr.AsClassicDataset(-1, 0)
	assert.Error(t, err)

	// classic datasets have no root group
	cds, err := Create(Memory, "", 1, Byte, 2, 2)
	require.NoError(t, err)
	defer cds.Close()
	_, err = cds.RootGroup()
	assert.Error(t, err)
	_, err = cds.RootGroup(ErrLogger(func(ec ErrorCategory, code int, msg string) error { return nil }))
	assert.Error(t, err)

	_, err = CreateMultiDimensional("notadriver", "")
	assert.Error(t, err)
	_, err = CreateMultiDimensional(GeoJSON, "")
	assert.Error(t, err)
}

func TestMultiDimViewAndClassicDataset(t *testing.T) {
	ds, root, arr := createMemMultiDim(t)
	defer ds.Close()
	defer root.Close()
	defer arr.Close()

	// second time step as a (lat, lon) array
	view, err := arr.View("[1,...]")
	require.NoError(t, err)
	defer view.Close()
	shape, err := view.Shape()
	require.NoError(t, err)
	assert.Equal(t, []uint64{3, 4}, shape)
	row := make([]float32, 4)
	require.NoError(t, view.Read([]int{2, 0}, []int{1, 4}, row))
	assert.Equal(t, []float32{120, 121, 122, 123}, row)

	cds, err := view.AsClassicDataset(1, 0)
	require.NoError(t, err)
	defer cds.Close()
	st := cds.Structure()
	assert.Equal(t, 4, st.SizeX)
	assert.Equal(t, 3, st.SizeY)
	assert.Equal(t, 1, st.NBands)
	pix := make([]float32, 12)
	require.NoError(t, cds.Read(0, 0, pix, 4, 3))
	assert.Equal(t, float32(100), pix[0])
	assert.Equal(t, float32(123), pix[11])

	// the full 3D array: extra dimensions become bands
	cds3, err := arr.AsClassicDataset(2, 1)
	require.NoError(t, err)
	defer cds3.Close()
	assert.Equal(t, 2, cds3.Structure().NBands)
}

func TestMultiDimOpenVRT(t *testing.T) {
	ds, err := Open("testdata/multidim.vrt", MultiDimRaster())
	require.NoError(t, err)
	defer ds.Close()
	root, err := ds.RootGroup()
	require.NoError(t, err)
	defer root.Close()

	attrs, err := root.Attributes()
	require.NoError(t, err)
	require.Len(t, attrs, 1)
	assert.Equal(t, "Conventions", attrs[0].Name)
	assert.Equal(t, "CF-1.8", attrs[0].String())

	names, err := root.MDArrayNames()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"time", "lat", "lon", "t2m"}, names)

	arr, err := root.OpenMDArray("t2m")
	require.NoError(t, err)
	defer arr.Close()
	assert.Equal(t, Int16, arr.DataType())
	assert.Equal(t, "K", arr.Unit())
	nd, ok := arr.NoData()
	assert.True(t, ok)
	assert.Equal(t, -9999.0, nd)
	scale, offset := arr.ScaleOffset()
	assert.Equal(t, 0.01, scale)
	assert.Equal(t, 273.15, offset)

	dims, err := arr.Dimensions()
	require.NoError(t, err)
	require.Len(t, dims, 3)
	assert.Equal(t, "time", dims[0].Name)
	assert.Equal(t, "/time", dims[0].IndexingVariable)
	assert.Equal(t, "NORTH", dims[1].Direction)
	assert.Equal(t, "/lat", dims[1].IndexingVariable)
	assert.Equal(t, "/lon", dims[2].IndexingVariable)

	// follow the indexing variable to read the coordinates of the lon axis
	lon, err := root.OpenMDArray(filepath.Base(dims[2].IndexingVariable))
	require.NoError(t, err)
	defer lon.Close()
	lons := make([]float64, 4)
	require.NoError(t, lon.Read([]int{0}, []int{4}, lons))
	assert.Equal(t, []float64{20, 21, 22, 23}, lons)

	vals := make([]int16, 4)
	require.NoError(t, arr.Read([]int{1, 2, 0}, []int{1, 1, 4}, vals, ConfigOption("GDAL_CACHEMAX=64")))
	assert.Equal(t, []int16{120, 121, 122, -9999}, vals)

	sr := arr.SpatialRef()
	require.NotNil(t, sr)
	assert.True(t, sr.Geographic())
	sr.Close()

	aattrs, err := arr.Attributes()
	require.NoError(t, err)
	require.Len(t, aattrs, 1)
	assert.Equal(t, "2 metre temperature", aattrs[0].String())

	sg, err := root.OpenGroup("surface")
	require.NoError(t, err)
	defer sg.Close()
	mask, err := sg.OpenMDArray("mask")
	require.NoError(t, err)
	defer mask.Close()
	mbuf := make([]byte, 12)
	require.NoError(t, mask.Read([]int{0, 0}, []int{3, 4}, mbuf))
	for _, m := range mbuf {
		assert.Equal(t, byte(1), m)
	}

	// read-only dataset
	assert.Error(t, arr.Write([]int{0, 0, 0}, []int{1, 1, 1}, []int16{1}))

	// opening a multidimensional VRT without MultiDimRaster gives no root group
	cds, err := Open("testdata/multidim.vrt", RasterOnly(), ErrLogger(func(ec ErrorCategory, code int, msg string) error { return nil }))
	if err == nil && cds != nil {
		_, err = cds.RootGroup()
		assert.Error(t, err)
		cds.Close()
	}
}

func TestMultiDimZarrRoundTrip(t *testing.T) {
	if err := RegisterRaster("Zarr"); err != nil {
		t.Skip("Zarr driver not available")
	}
	dir, err := os.MkdirTemp("", "godal-zarr")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	name := filepath.Join(dir, "forecast.zarr")

	ds, err := CreateMultiDimensional("Zarr", name, CreationOption("FORMAT=ZARR_V2"), ConfigOption("CPL_DEBUG=OFF"))
	require.NoError(t, err)
	root, err := ds.RootGroup()
	require.NoError(t, err)
	require.NoError(t, root.CreateDimension(Dimension{Name: "lat", Size: 2}))
	require.NoError(t, root.CreateDimension(Dimension{Name: "lon", Size: 3}))
	arr, err := root.CreateMDArray("precip", []string{"lat", "lon"}, Float32)
	require.NoError(t, err)
	require.NoError(t, arr.SetAttribute("long_name", "precipitation"))
	require.NoError(t, arr.Write([]int{0, 0}, []int{2, 3}, []float32{0, 0.5, 1, 1.5, 2, 2.5}))
	arr.Close()
	root.Close()
	require.NoError(t, ds.Close())

	ds, err = Open(name, MultiDimRaster())
	require.NoError(t, err)
	defer ds.Close()
	root, err = ds.RootGroup()
	require.NoError(t, err)
	defer root.Close()
	arr, err = root.OpenMDArray("precip")
	require.NoError(t, err)
	defer arr.Close()
	assert.Equal(t, Float32, arr.DataType())
	vals := make([]float32, 3)
	require.NoError(t, arr.Read([]int{1, 0}, []int{1, 3}, vals))
	assert.Equal(t, []float32{1.5, 2, 2.5}, vals)
	attrs, err := arr.Attributes()
	require.NoError(t, err)
	found := false
	for _, a := range attrs {
		if a.Name == "long_name" {
			found = true
			assert.Equal(t, "precipitation", a.String())
		}
	}
	assert.True(t, found)
}
