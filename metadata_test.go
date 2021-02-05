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

func TestMetadata(t *testing.T) {
	tmpfname := tempfile()
	defer os.Remove(tmpfname)
	ds, _ := Create(GTiff, tmpfname, 1, Byte, 10, 10)

	md1 := ds.Metadata("foo")
	if md1 != "" {
		t.Error(md1)
	}
	md1 = ds.Metadata("foo", Domain("bar"))
	if md1 != "" {
		t.Error(md1)
	}
	err := ds.SetMetadata("foo", "bar")
	if err != nil {
		t.Error(err)
	}
	err = ds.SetMetadata("foo2", "bar2", Domain("baz"))
	if err != nil {
		t.Error(err)
	}
	md1 = ds.Metadata("foo")
	if md1 != "bar" {
		t.Error(md1)
	}
	md1 = ds.Metadata("foo2", Domain("baz"))
	if md1 != "bar2" {
		t.Error(md1)
	}

	mds := ds.Metadatas()
	if len(mds) != 1 {
		t.Error("empty")
	}
	for k, v := range mds {
		if k != "foo" || v != "bar" {
			t.Errorf("%s = %s", k, v)
		}
	}
	mds = ds.Metadatas(Domain("baz"))
	if len(mds) != 1 {
		t.Error("empty")
	}
	for k, v := range mds {
		if k != "foo2" || v != "bar2" {
			t.Errorf("%s = %s", k, v)
		}
	}
	mds = ds.Metadatas(Domain("bogus"))
	if len(mds) != 0 {
		t.Error("not empty")
	}

	_ = ds.SetMetadata("empty", "", Domain("empty"))
	mds = ds.Metadatas(Domain("empty"))
	if len(mds) != 1 {
		t.Errorf("empty: %d", len(mds))
	}
	for k, v := range mds {
		if k != "empty" || v != "" {
			t.Errorf("%s = %s", k, v)
		}
	}

	domains := ds.MetadataDomains()
	assert.Contains(t, domains, "")
	assert.Contains(t, domains, "empty")
	assert.Contains(t, domains, "baz")

	ds.Close()
	err = ds.SetMetadata("foo", "bar")
	assert.Error(t, err)

}
