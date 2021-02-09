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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

type bufAdapter []byte
type mbufAdapter struct {
	bufAdapter
}

func (b bufAdapter) ReadAt(buf []byte, off int64) (int, error) {
	if int(off) >= len(b) {
		return 0, io.EOF
	}
	n := copy(buf, b[off:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}
func (mb mbufAdapter) ReadAtMulti(bufs [][]byte, offs []int64) ([]int, error) {
	ret := make([]int, len(bufs))
	var err error
	for i := range bufs {
		ret[i], err = mb.ReadAt(bufs[i], offs[i])
		if err != nil {
			return ret, err
		}
	}
	return ret, nil
}
func (b bufAdapter) Size() (uint64, error) {
	return uint64(len(b)), nil
}

type vpAdapter struct {
	datas map[string]VSIReader
}

func (vp vpAdapter) VSIReader(k string) (VSIReader, error) {
	b, ok := vp.datas[k]
	if !ok {
		return nil, syscall.ENOENT
	}
	return b, nil
}

func TestVSIPlugin(t *testing.T) {
	vpa := vpAdapter{datas: make(map[string]VSIReader)}
	tifdat, _ := ioutil.ReadFile("testdata/test.tif")
	vpa.datas["test.tif"] = mbufAdapter{tifdat}
	err := RegisterVSIHandler("testmem://", vpa)
	assert.NoError(t, err)
	err = RegisterVSIHandler("testmem://", vpa)
	assert.Error(t, err)
	err = RegisterVSIHandler("/vsimem/", vpa)
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
	vpa := vpAdapter{datas: make(map[string]VSIReader)}
	tifdat, _ := ioutil.ReadFile("testdata/test.tif")
	vpa.datas["test.tif"] = mbufAdapter{tifdat}
	_ = RegisterVSIHandler("testmem2://", vpa, VSIHandlerBufferSize(10), VSIHandlerCacheSize(30))

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
	vpa := vpAdapter{datas: make(map[string]VSIReader)}
	tifdat, _ := ioutil.ReadFile("testdata/test.tif")
	vpa.datas["test.tif"] = bufAdapter(tifdat)
	_ = RegisterVSIHandler("testmem3://", vpa, VSIHandlerBufferSize(10), VSIHandlerCacheSize(30))

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

type sizeErroringAdapter struct {
	bufAdapter
}
type readErroringAdapter struct {
	bufAdapter
}
type bodyreadErroringAdapter struct {
	bufAdapter
}
type multireadErroringAdapter struct {
	bufAdapter
}

func (se sizeErroringAdapter) Size() (uint64, error) {
	return 0, fmt.Errorf("not implemented")
}
func (re readErroringAdapter) ReadAt(buf []byte, off int64) (int, error) {
	return 0, fmt.Errorf("not implemented")
}
func (re bodyreadErroringAdapter) ReadAt(buf []byte, off int64) (int, error) {
	if off >= 2230 { //2230 is the offset of the strile data in tt.tif
		return 0, fmt.Errorf("read >414 not implemented")
	}
	return re.bufAdapter.ReadAt(buf, off)
}
func (re multireadErroringAdapter) ReadAtMulti(bufs [][]byte, offs []int64) ([]int, error) {
	return nil, fmt.Errorf("mr not implemented")
}

func TestVSIErrors(t *testing.T) {
	tt := tempfile()
	defer os.Remove(tt)
	ds, _ := Create(GTiff, tt, 3, Byte, 2048, 2048, CreationOption("TILED=YES", "COMPRESS=LZW", "BLOCKXSIZE=128", "BLOCKYSIZE=128"))
	ds.Close()
	vpa := vpAdapter{datas: make(map[string]VSIReader)}
	tifdat, _ := ioutil.ReadFile(tt)
	vpa.datas["test.tif"] = sizeErroringAdapter{bufAdapter(tifdat)}
	vpa.datas["test2.tif"] = readErroringAdapter{bufAdapter(tifdat)}
	vpa.datas["test3.tif"] = multireadErroringAdapter{bufAdapter(tifdat)}
	vpa.datas["test4.tif"] = bodyreadErroringAdapter{bufAdapter(tifdat)}
	_ = RegisterVSIHandler("testmem4://", vpa, VSIHandlerBufferSize(0), VSIHandlerCacheSize(0))

	_, err := Open("testmem4://test.tif")
	if err == nil {
		t.Error("err not raised")
	}
	_, err = Open("testmem4://test2.tif")
	if err == nil {
		t.Error("err not raised")
	}
	data := make([]byte, 300)

	ds, err = Open("testmem4://test3.tif")
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
