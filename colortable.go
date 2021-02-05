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

//#include "godal.h"
import "C"
import (
	"errors"
	"unsafe"
)

//PaletteInterp defines the color interpretation of a ColorTable
type PaletteInterp C.GDALPaletteInterp

const (
	//GrayscalePalette is a grayscale palette with a single component per entry
	GrayscalePalette PaletteInterp = C.GPI_Gray
	//RGBPalette is a RGBA palette with 4 components per entry
	RGBPalette PaletteInterp = C.GPI_RGB
	//CMYKPalette is a CMYK palette with 4 components per entry
	CMYKPalette PaletteInterp = C.GPI_CMYK
	//HLSPalette is a HLS palette with 3 components per entry
	HLSPalette PaletteInterp = C.GPI_HLS
)

//ColorTable is a color table associated with a Band
type ColorTable struct {
	PaletteInterp PaletteInterp
	Entries       [][4]int16
}

func cColorTableArray(in [][4]int16) *C.short {
	ret := make([]C.short, len(in)*4)
	for i := range in {
		ret[4*i] = C.short(in[i][0])
		ret[4*i+1] = C.short(in[i][1])
		ret[4*i+2] = C.short(in[i][2])
		ret[4*i+3] = C.short(in[i][3])
	}
	return (*C.short)(unsafe.Pointer(&ret[0]))
}

func ctEntriesFromCshorts(arr *C.short, nEntries int) [][4]int16 {
	int16s := (*[1 << 30]C.short)(unsafe.Pointer(arr))
	ret := make([][4]int16, nEntries)
	for i := 0; i < nEntries; i++ {
		ret[i][0] = int16(int16s[i*4])
		ret[i][1] = int16(int16s[i*4+1])
		ret[i][2] = int16(int16s[i*4+2])
		ret[i][3] = int16(int16s[i*4+3])
	}
	return ret
}

//ColorTable returns the bands color table. The returned ColorTable will have
//a 0-length Entries if the band has no color table assigned
func (band Band) ColorTable() ColorTable {
	var interp C.GDALPaletteInterp
	var nEntries C.int
	var cEntries *C.short
	C.godalGetColorTable(band.Handle(), &interp, &nEntries, &cEntries)
	if cEntries != nil {
		defer C.free(unsafe.Pointer(cEntries))
	}
	return ColorTable{
		PaletteInterp: PaletteInterp(interp),
		Entries:       ctEntriesFromCshorts(cEntries, int(nEntries)),
	}
}

// SetColorTable sets the band's color table. if passing in a 0-length ct.Entries,
// the band's color table will be cleared
func (band Band) SetColorTable(ct ColorTable) error {
	var cshorts *C.short
	if len(ct.Entries) > 0 {
		cshorts = cColorTableArray(ct.Entries)
	}
	errmsg := C.godalSetColorTable(band.Handle(), C.GDALPaletteInterp(ct.PaletteInterp), C.int(len(ct.Entries)), cshorts)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}
