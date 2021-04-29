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

package gcs_test

import (
	"context"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/airbusgeo/godal"
	"github.com/airbusgeo/godal/gcs"
	"github.com/airbusgeo/godal/pkg/blockcache"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
)

func TestVSIGCS(t *testing.T) {
	godal.RegisterAll()
	ctx := context.Background()
	_, err := storage.NewClient(ctx)
	if err != nil {
		t.Skipf("skip test on missing credentials: %v", err)
	}
	err = gcs.RegisterHandler(ctx, gcs.Prefix("gdalgs://"))
	if err != nil {
		t.Error(err)
	}
	ds, err := godal.Open("gdalgs://godal-ci-data/test.tif")
	if err != nil {
		t.Error(err)
		return
	}
	defer ds.Close()
	if ds.Structure().SizeX != 10 {
		t.Errorf("xsize: %d", ds.Structure().SizeX)
	}
	data := make([]byte, 100)
	err = ds.Read(0, 0, data, 10, 10)
	if err != nil {
		t.Error(err)
	}

	_, err = godal.Open("gdalgs://godal-ci-data/gdd/doesnotexist.tif")
	if err == nil {
		t.Error("ENOENT not raised")
	}
	_, err = godal.Open("gdalgs://godal-fake-test/gdaltesdata/doesnotexist.tif")
	if err == nil {
		t.Error("ENOENT not raised")
	}
}

func TestVSIGCSNoAuth(t *testing.T) {
	godal.RegisterAll()
	ctx := context.Background()
	st, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		t.Skipf("failed to create gcs client: %v", err)
	}
	err = gcs.RegisterHandler(ctx, gcs.Prefix("gdalgcs://"), gcs.Client(st))
	if err != nil {
		t.Error(err)
	}
	ds, err := godal.Open("gdalgcs://godal-ci-data/test.tif")
	if err != nil {
		t.Error(err)
		return
	}
	defer ds.Close()
	if ds.Structure().SizeX != 10 {
		t.Errorf("xsize: %d", ds.Structure().SizeX)
	}
	data := make([]byte, 100)
	err = ds.Read(0, 0, data, 10, 10)
	if err != nil {
		t.Error(err)
	}

	_, err = godal.Open("gdalgcs://godal-ci-data/gdd/doesnotexist.tif")
	if err == nil {
		t.Error("ENOENT not raised")
	}
	_, err = godal.Open("gdalgs://godal-fake-test/gdaltesdata/doesnotexist.tif")
	if err == nil {
		t.Error("ENOENT not raised")
	}
}

func TestVSICacher(t *testing.T) {
	cacher, _ := blockcache.NewCache(10)
	godal.RegisterInternalDrivers()
	ctx := context.Background()
	st, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		t.Skipf("failed to create gcs client: %v", err)
	}
	_ = gcs.RegisterHandler(ctx, gcs.Prefix("cc://"), gcs.Cacher(cacher), gcs.Client(st))
	ds, err := godal.Open("cc://godal-ci-data/test.tif")
	if err != nil {
		t.Error(err)
		return
	}
	defer ds.Close()
	_, ok := cacher.Get("godal-ci-data/test.tif", 0)
	assert.True(t, ok)
}
