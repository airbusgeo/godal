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
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	st := ds.Structure()
	if st.DataType != Unknown || st.NBands > 0 {
		t.Errorf("created raster %v", st)
	}
}
