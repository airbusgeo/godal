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

func TestVectorTranslate(t *testing.T) {
	tmpname := tempfile()
	defer os.Remove(tmpname)
	ds, err := Open("testdata/test.geojson", VectorOnly())
	if err != nil {
		t.Fatal(err)
	}
	st1, _ := os.Stat("testdata/test.geojson")
	nds, err := ds.VectorTranslate(tmpname, []string{"-lco", "RFC7946=YES"}, GeoJSON)
	if err != nil {
		t.Fatal(err)
	}
	_ = nds.SetMetadata("baz", "boo")
	err = nds.Close()
	if err != nil {
		t.Error("err")
	}
	st2, _ := os.Stat(tmpname)
	if st2.Size() == 0 || st1.Size() == st2.Size() {
		t.Error("invalid size")
	}

	err = RegisterVector("TAB")
	if err != nil {
		t.Fatal(err)
	}
	tmpdir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(tmpdir)
	mds, err := ds.VectorTranslate(filepath.Join(tmpdir, "test.mif"), []string{"-f", "Mapinfo File"}, CreationOption("FORMAT=MIF"))
	if err != nil {
		t.Error(err)
	} else {
		mds.Close()
	}

	_, err = ds.VectorTranslate("foobar", []string{"-f", "bogusdriver"})
	if err == nil {
		t.Error("err not raised")
	}
}

func TestVectorLayer(t *testing.T) {
	rds, _ := Create(Memory, "", 3, Byte, 10, 10)
	_, err := rds.CreateLayer("ff", nil, GTPolygon)
	if err == nil {
		t.Error("error not raised")
	}
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
	assert.Error(t, ds.SetNoData(0))
	buf := make([]byte, 10)
	assert.Error(t, ds.Read(0, 0, buf, 3, 3))
	assert.Error(t, ds.Write(0, 0, buf, 3, 3))

	dds, err := ds.VectorTranslate("", []string{"-of", "MEMORY"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = (&Geometry{}).Buffer(10, 1)
	assert.Error(t, err)
	_, err = (&Geometry{}).Simplify(1)
	assert.Error(t, err)

	err = (&Feature{}).SetGeometry(&Geometry{})
	assert.Error(t, err)

	sr4326, _ := NewSpatialRefFromEPSG(4326)
	defer sr4326.Close()
	sr3857, _ := NewSpatialRefFromEPSG(3857)
	defer sr3857.Close()
	l2, err := dds.CreateLayer("t2", sr4326, GTPoint)
	assert.NoError(t, err)
	assert.True(t, sr4326.IsSame(l2.SpatialRef()))
	l := dds.Layers()[0]
	l.ResetReading()
	cnt, err := l.FeatureCount()
	assert.NoError(t, err)
	_, err = Layer{}.FeatureCount()
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
		if err != nil {
			t.Error(err)
		}
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
		err = l.UpdateFeature(ff)
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

	gp, err := NewGeometryFromWKT("POINT (30 10)", nil)
	assert.NoError(t, err)
	assert.NotNil(t, gp)

	wkt, err := gp.WKT()
	assert.NoError(t, err)
	assert.Equal(t, "POINT (30 10)", wkt)

	wkb, err := gp.WKB()
	assert.NoError(t, err)
	assert.NotEmpty(t, wkb)

	gp.Close()

	_, err = NewGeometryFromWKB(wkb[0:10], &SpatialRef{})
	assert.Error(t, err)
	gp, err = NewGeometryFromWKB(wkb, nil)
	assert.NoError(t, err)
	assert.NotNil(t, gp)

	wkt, err = gp.WKT()
	assert.NoError(t, err)
	assert.Equal(t, "POINT (30 10)", wkt)

	//nil geom for wkb is ok
	_, err = (&Geometry{}).WKB()
	assert.NoError(t, err)

	_, err = (&Geometry{}).WKT()
	assert.Error(t, err)
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
				"floatCol":123.4
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
		NewFieldDefinition("ignored", FieldType(FTInt64List)),
	)
	assert.NoError(t, err)

	pnt, _ := NewGeometryFromWKT("POINT (1 1)", nil)
	nf, err := lyr.NewFeature(pnt)
	assert.NoError(t, err)
	attrs := nf.Fields()
	sfield := attrs["strCol"]
	assert.Equal(t, FTString, sfield.Type())
	assert.Equal(t, "", sfield.String())
	assert.Equal(t, int64(0), sfield.Int())
	assert.Equal(t, 0.0, sfield.Float())
	sfield = attrs["intCol"]
	assert.Equal(t, FTInt, sfield.Type())
	assert.Equal(t, "0", sfield.String())
	assert.Equal(t, int64(0), sfield.Int())
	assert.Equal(t, 0.0, sfield.Float())
	sfield = attrs["int64Col"]
	assert.Equal(t, FTInt64, sfield.Type())
	assert.Equal(t, "0", sfield.String())
	assert.Equal(t, int64(0), sfield.Int())
	assert.Equal(t, 0.0, sfield.Float())
	sfield = attrs["floatCol"]
	assert.Equal(t, FTReal, sfield.Type())
	assert.Equal(t, "0.000000", sfield.String())
	assert.Equal(t, int64(0), sfield.Int())
	assert.Equal(t, 0.0, sfield.Float())

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
