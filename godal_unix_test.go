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

//go:build unix

package godal

import (
	"math"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpacedIOBeyond32Bits(t *testing.T) {
	if math.MaxInt == math.MaxInt32 {
		t.Skip("a spacing beyond 32 bits needs a 64-bit int")
	}
	ds, _ := Create(Memory, "", 2, Byte, 2, 2)
	defer func() {
		_ = ds.Close()
	}()
	require.NoError(t, ds.Bands()[0].Fill(1, 0))
	require.NoError(t, ds.Bands()[1].Fill(2, 0))

	// A spacing that wraps to 4 when it is truncated to a 32-bit C int.
	// The shift count is a variable so that this file compiles with a 32-bit int.
	shift := 32
	spacing := 1<<shift + 4

	// The buffer is an anonymous mapping and not a Go slice: the kernel commits only
	// the pages that are written to, whereas the Go allocator can zero the whole slice.
	buf, err := syscall.Mmap(-1, 0, spacing+4, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	require.NoError(t, err)
	defer func() {
		_ = syscall.Munmap(buf)
	}()

	err = ds.Bands()[0].Read(0, 0, buf, 2, 2, LineSpacing(spacing))
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 1, 0, 0, 0, 0, 0, 0}, buf[:8])
	assert.Equal(t, []byte{1, 1, 0, 0}, buf[spacing:])

	// This read overwrites all the bytes that the band read wrote.
	err = ds.Read(0, 0, buf, 2, 2, BandSpacing(spacing), PixelSpacing(1), LineSpacing(2))
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 1, 1, 1, 0, 0, 0, 0}, buf[:8])
	assert.Equal(t, []byte{2, 2, 2, 2}, buf[spacing:])
}
