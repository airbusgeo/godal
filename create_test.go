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
)

func TestCreate(t *testing.T) {
	tmpname := tempfile()
	defer os.Remove(tmpname)

	_, err := Create(GTiff, tmpname, 1, Byte, 20, 20, CreationOption("INVALID_OPT=BAR"))
	if err == nil {
		t.Error("invalid copt not raised")
	}
	_, err = Create(GTiff, "/this/path/does/not/exist", 1, Byte, 10, 10)
	if err == nil {
		t.Error("error not caught")
	}

	ds, err := Create(GTiff, tmpname, 1, Byte, 20, 20)
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Error(err)
	}
	ci = bnds[0].ColorInterp()
	if ci != CIRed {
		t.Error(ci.Name())
	}
	err = ds.Close()
	if err != nil {
		t.Error(err)
	}
	st1, _ := os.Stat(tmpname)
	tmpname2 := tempfile()
	defer os.Remove(tmpname2)
	ds, err = Create(GTiff, tmpname2, 1, Byte, 20, 20, CreationOption("TILED=YES", "BLOCKXSIZE=128", "BLOCKYSIZE=128"))
	if err != nil {
		t.Fatal(err)
	}
	err = ds.Close()
	if err != nil {
		t.Error(err)
	}
	st2, _ := os.Stat(tmpname2)

	if st1.Size() == st2.Size() {
		t.Errorf("sizes: %d/%d", st1.Size(), st2.Size())
	}
}
