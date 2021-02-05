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
	"testing"

	"github.com/stretchr/testify/assert"
)

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
	err = VSIUnlink(fname)
	assert.Error(t, err)
}
