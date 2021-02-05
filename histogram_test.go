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

func TestHistogram(t *testing.T) {
	ds, _ := Create(Memory, "", 1, Byte, 16, 16)
	defer ds.Close()
	buf := make([]byte, 256)
	for i := range buf {
		buf[i] = byte(i)
	}
	_ = ds.Write(0, 0, buf, 16, 16)
	bnd := ds.Bands()[0]

	hist, err := bnd.Histogram()
	assert.NoError(t, err)
	ll := hist.Len()
	assert.Equal(t, 256, ll)
	for i := 0; i < ll; i++ {
		b := hist.Bucket(i)
		assert.Equal(t, float64(i)-0.5, b.Min)
		assert.Equal(t, float64(i+1)-0.5, b.Max)
		assert.Equal(t, uint64(1), b.Count)
	}

	hist, err = bnd.Histogram(IncludeOutOfRange(), Intervals(64, 63.5, 191.5))
	assert.NoError(t, err)
	ll = hist.Len()
	assert.Equal(t, 64, ll)
	for i := 0; i < ll; i++ {
		b := hist.Bucket(i)
		assert.Equal(t, 63.5+float64(i*2), b.Min)
		assert.Equal(t, 63.5+float64(i*2+2), b.Max)
		if i == 0 || i == ll-1 {
			assert.Equal(t, uint64(66), b.Count) //66 is the 64 preceding + the 2 of the actual bucket
		} else {
			assert.Equal(t, uint64(2), b.Count)
		}
	}
	_, err = bnd.Histogram(Approximate(), Intervals(64, 64, 192))
	assert.NoError(t, err)

	//to make histogram choke for coverage
	ebnd := Band{}
	_, err = ebnd.Histogram()
	assert.Error(t, err)
}
