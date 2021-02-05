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

func TestCBuffer(t *testing.T) {
	bbuf := make([]byte, 100)
	sz, dt, _ := cBuffer(bbuf)
	if dt != Byte || sz != 1 {
		t.Error("cbuf bug")
	}
	i16buf := make([]int16, 100)
	sz, dt, _ = cBuffer(i16buf)
	if dt != Int16 || sz != 2 {
		t.Error("cbuf bug")
	}
	u16buf := make([]uint16, 100)
	sz, dt, _ = cBuffer(u16buf)
	if dt != UInt16 || sz != 2 {
		t.Error("cbuf bug")
	}
	i32buf := make([]int32, 100)
	sz, dt, _ = cBuffer(i32buf)
	if dt != Int32 || sz != 4 {
		t.Error("cbuf bug")
	}
	u32buf := make([]uint32, 100)
	sz, dt, _ = cBuffer(u32buf)
	if dt != UInt32 || sz != 4 {
		t.Error("cbuf bug")
	}
	f32buf := make([]float32, 100)
	sz, dt, _ = cBuffer(f32buf)
	if dt != Float32 || sz != 4 {
		t.Error("cbuf bug")
	}
	f64buf := make([]float64, 100)
	sz, dt, _ = cBuffer(f64buf)
	if dt != Float64 || sz != 8 {
		t.Error("cbuf bug")
	}
	c64buf := make([]complex64, 100)
	sz, dt, _ = cBuffer(c64buf)
	if dt != CFloat32 || sz != 8 {
		t.Error("cbuf bug")
	}
	c128buf := make([]complex128, 100)
	sz, dt, _ = cBuffer(c128buf)
	if dt != CFloat64 || sz != 16 {
		t.Error("cbuf bug")
	}

	assert.Panics(t, func() { cBuffer("stringtest") })
}
