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

func TestTransform(t *testing.T) {
	sr1, _ := NewSpatialRefFromEPSG(4326)
	sr2, _ := NewSpatialRefFromEPSG(3857)
	ct, err := NewTransform(sr1, sr2)
	if err != nil {
		t.Fatal(err)
	}
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

	pj, _ := ds.SpatialRef().WKT()

	if pj != epsg4326 {
		t.Error(pj)
	}

	_, err = NewSpatialRefFromProj4("invalid string")
	if err == nil {
		t.Error("invalid proj4 not raised")
	}
	sr, err = NewSpatialRefFromProj4("+proj=lonlat")
	if err != nil {
		t.Error(err)
	}
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
	sr, err = NewSpatialRefFromWKT(epsg4326)
	if err != nil {
		t.Error(err)
	}
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
	if err == nil {
		t.Error("invalid wkt not raised")
	}
	err = ds.SetProjection(epsg4326)
	if err != nil {
		t.Error(err)
	}
	pj = ds.Projection()
	if pj != epsg4326 {
		t.Error(pj)
	}

	err = ds.SetSpatialRef(nil)
	if err != nil {
		t.Error(err)
	}
	pj = ds.Projection()
	if pj != "" {
		t.Errorf("proj not empty: %s", pj)
	}

	//hack to make setspatialref return an error for coverage
	ds.Close()
	err = ds.SetSpatialRef(nil)
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
	if !epsg4326.IsSame(sr) {
		t.Error("isSame failed")
	}
	if !epsg4326.Geographic() {
		t.Error("not geographic")
	}
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

	err = sr.AutoIdentifyEPSG()
	assert.NoError(t, err)

	l, err := NewSpatialRefFromWKT(`LOCAL_CS[,UNIT["m",1]]`)
	assert.NoError(t, err)
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
	_, err = ds.Bounds()
	assert.Error(t, err)
	ngt := [6]float64{0, 2, 1, 0, 1, 1}

	err = ds.SetGeoTransform(ngt)
	assert.NoError(t, err)

	gt, err := ds.GeoTransform()
	assert.NoError(t, err)
	assert.Equal(t, gt, ngt)
}

func TestGeometryTransform(t *testing.T) {
	sr, _ := NewSpatialRefFromEPSG(4326)
	srm, _ := NewSpatialRefFromEPSG(3857)
	gp, _ := NewGeometryFromWKT("POINT (10 10)", sr)
	assert.True(t, gp.SpatialRef().IsSame(sr))

	err := gp.Reproject(srm)
	assert.NoError(t, err)
	assert.True(t, gp.SpatialRef().IsSame(srm))
	nwkt, _ := gp.WKT()
	assert.NotEqual(t, "POINT (10 10)", nwkt)

	gp.SetSpatialRef(sr)
	assert.True(t, gp.SpatialRef().IsSame(sr))

	gp.Close()

	gp, _ = NewGeometryFromWKT("POINT (10 90)", sr)
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

	gp, _ = NewGeometryFromWKT("POINT (10 90)", sr)
	err = gp.Transform(trn)
	assert.Error(t, err)
}

func TestProjBounds(t *testing.T) {
	sr4326, _ := NewSpatialRefFromEPSG(4326)
	sr3857, _ := NewSpatialRefFromEPSG(3857)
	box, err := NewGeometryFromWKT("POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))", sr4326)
	assert.NoError(t, err)
	_, err = box.Bounds(sr3857)
	assert.Error(t, err)
	_, err = box.Bounds(&SpatialRef{handle: nil})
	assert.Error(t, err)

}
