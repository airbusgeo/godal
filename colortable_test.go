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
	"testing"

	"github.com/stretchr/testify/assert"
)

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
	assert.Equal(t, CIUndefined, bnd.ColorInterp())

	ds, _ = Open("testdata/test.tif")
	defer ds.Close()
	bnd = ds.Bands()[0]
	err = bnd.SetColorTable(ct)
	assert.Error(t, err) //read-only
}
