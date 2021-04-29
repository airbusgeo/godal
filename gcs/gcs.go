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

package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"syscall"

	"github.com/airbusgeo/godal"
	"github.com/airbusgeo/godal/internal/blockcache"

	"cloud.google.com/go/storage"
	lru "github.com/hashicorp/golang-lru"
	"google.golang.org/api/googleapi"
)

type gcsHandler struct {
	ctx                context.Context
	prefix             string
	client             *storage.Client
	cacher             blockcache.Cacher
	blockSize          int
	maxCachedBlocks    int
	maxCachedMetadatas int
	handleBufferSize   int
	handleCacheSize    int
	blockCache         *blockcache.BlockCache
	sizecache          *lru.Cache
	billingProjectID   string
	splitRanges        bool
}

//Option is an option that can be passed to RegisterHandler
type Option func(o *gcsHandler)

// Prefix is the prefix that a file must have in order to be handled by this handler
// Defaults to "gs://", i.e. this handler will be used when calling godal.Open("gs://mybucket/myfile.tif")
func Prefix(prefix string) Option {
	return func(o *gcsHandler) {
		o.prefix = prefix
	}
}

// Client sets the cloud.google.com/go/storage.Client that will be used
// by the handler
func Client(cl *storage.Client) Option {
	return func(o *gcsHandler) {
		o.client = cl
	}
}

// Cacher allows to plugin a custom cache mechanism instead of the default in
// memory lru cache. MaxCachedBlocks() will not be honored if you provide your
// own cacher, it is up to your cacher implementation to handle block eviction
func Cacher(cacher blockcache.Cacher) Option {
	return func(o *gcsHandler) {
		o.cacher = cacher
	}
}

// BlockSize sets the size of requests that will go out to the storage API.
// Defaults to 1Mb
func BlockSize(bs int) Option {
	if bs < 1 {
		panic("invalid blocksize")
	}
	return func(o *gcsHandler) {
		o.blockSize = bs
	}
}

// MaxCachedBlocks sets the number of blocks to keep in the lru cache.
// Defaults to 1000
func MaxCachedBlocks(n int) Option {
	if n < 1 {
		panic("inavlid max cached blocks")
	}
	return func(o *gcsHandler) {
		o.maxCachedBlocks = n
	}
}
func VSIHandleBuffer(n int) Option {
	if n != 0 && n < 1024 {
		panic("invalid handle buffer")
	}
	return func(o *gcsHandler) {
		o.handleBufferSize = n
	}
}
func VSIHandleCache(n int) Option {
	if n != 0 && n < 1024 {
		panic("invalid handle buffer")
	}
	return func(o *gcsHandler) {
		o.handleCacheSize = n
	}
}

// BillingProject sets the project name which should be billed for the requests.
// This is mandatory if the bucket is in requester-pays mode.
func BillingProject(projectID string) Option {
	return func(o *gcsHandler) {
		o.billingProjectID = projectID
	}
}

//SplitConsecutiveRanges forces multiple parallel requests for individual blocks
//when a requested chunk spans multiple blocks, instead of emitting a single request
//spanning multiple blocks. Can be useful for e.g. a tile server processing concurrent
//requests on neighbouring image regions.
func SplitConsecutiveRanges(split bool) Option {
	return func(o *gcsHandler) {
		o.splitRanges = split
	}
}

//MaxCachedMetadatas sets the number of filenames whose size will be kept in cache.
//This also accounts for non-existing files (i.e. calling Open() twice on a non-exisiting file
//will not result in an API call going to the storage endpoint the second time
func MaxCachedMetadatas(n int) Option {
	if n < 1 {
		panic("invalid max cached metadatas")
	}
	return func(o *gcsHandler) {
		o.maxCachedMetadatas = n
	}
}

// RegisterHandler registers a vsi handler to gdal in order to use cloud.google.com/go/storage
// APIs to access objects on cloud storage buckets
func RegisterHandler(ctx context.Context, opts ...Option) error {
	handler := &gcsHandler{
		ctx:                ctx,
		prefix:             "gs://",
		blockSize:          1024 * 1024,
		maxCachedBlocks:    1000,
		handleBufferSize:   64 * 1024,
		handleCacheSize:    64 * 1024 * 2,
		maxCachedMetadatas: 10000,
	}
	for _, o := range opts {
		o(handler)
	}
	handler.sizecache, _ = lru.New(handler.maxCachedMetadatas)
	if handler.client == nil {
		cl, err := storage.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("storage.newclient: %w", err)
		}
		handler.client = cl
	}
	if handler.cacher == nil {
		handler.cacher, _ = blockcache.NewCache(uint(handler.maxCachedBlocks))
	}
	handler.blockCache = blockcache.New(handler, handler.cacher, uint(handler.blockSize), handler.splitRanges)
	return godal.RegisterVSIHandler(handler.prefix, handler,
		godal.VSIHandlerBufferSize(handler.handleBufferSize),
		godal.VSIHandlerCacheSize(handler.handleCacheSize))
}

func gcsparse(gsUri string) (bucket, object string) {
	if gsUri[0] == '/' {
		gsUri = gsUri[1:]
	}
	firstSlash := strings.Index(gsUri, "/")
	if firstSlash == -1 {
		bucket = gsUri
		object = ""
	} else {
		bucket = gsUri[0:firstSlash]
		object = gsUri[firstSlash+1:]
	}
	return
}

func (gcs *gcsHandler) precheck(key string, off int64) error {
	s, ok := gcs.sizecache.Get(key)
	if ok {
		s64 := s.(int64)
		if s64 == -1 {
			return syscall.ENOENT
		}
		if off >= s64 {
			return io.EOF
		}
	}
	return nil
}

func (gcs *gcsHandler) ReadAt(key string, p []byte, off int64) (int, error) {
	if err := gcs.precheck(key, off); err != nil {
		return 0, err
	}
	bucket, object := gcsparse(key)
	if len(bucket) == 0 || len(object) == 0 {
		return 0, fmt.Errorf("invalid key")
	}
	gbucket := gcs.client.Bucket(bucket)
	if gcs.billingProjectID != "" {
		gbucket = gbucket.UserProject(gcs.billingProjectID)
	}
	r, err := gbucket.Object(object).NewRangeReader(gcs.ctx, off, int64(len(p)))
	//fmt.Printf("read %s [%d-%d]\n", key, off, off+int64(len(p)))
	if err != nil {
		var gerr *googleapi.Error
		if off > 0 && errors.As(err, &gerr) && gerr.Code == 416 {
			return 0, io.EOF
		}
		if off == 0 && errors.Is(err, storage.ErrObjectNotExist) {
			gcs.sizecache.Add(key, int64(-1))
			return 0, syscall.ENOENT
		}
		return 0, fmt.Errorf("new reader for gs://%s/%s: %w", bucket, object, err)
	}
	if sz := r.Attrs.Size; sz > 0 {
		gcs.sizecache.Add(key, sz)
	}
	defer r.Close()
	n, err := io.ReadFull(r, p)
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	return n, err
}

func (gcs *gcsHandler) VSIReader(key string) (godal.VSIReader, error) {
	//log.Printf("vsireader called on %s", key)
	return gcsObjectReader{key: key, gcs: gcs}, nil
}

type gcsObjectReader struct {
	key string
	gcs *gcsHandler
}

func (v gcsObjectReader) ReadAt(buf []byte, off int64) (int, error) {
	if err := v.gcs.precheck(v.key, off); err != nil {
		return 0, err
	}
	return v.gcs.blockCache.ReadAt(v.key, buf, off)
}

func (v gcsObjectReader) ReadAtMulti(bufs [][]byte, offs []int64) ([]int, error) {
	s, ok := v.gcs.sizecache.Get(v.key)
	if ok {
		s64 := s.(int64)
		if s64 == -1 {
			return nil, syscall.ENOENT
		}
		for _, off := range offs {
			if off >= s64 {
				return nil, io.EOF
			}
		}
	}
	return v.gcs.blockCache.ReadAtMulti(v.key, bufs, offs)
}

func (v gcsObjectReader) Size() (uint64, error) {
	s, ok := v.gcs.sizecache.Get(v.key)
	if !ok {
		buf := make([]byte, 1)
		_, _ = v.ReadAt(buf, 0) //ignore errors as we just want to populate the size cache
		s, ok = v.gcs.sizecache.Get(v.key)
	}
	if ok {
		size := s.(int64)
		if size == -1 {
			return 0, syscall.ENOENT
		}
		return uint64(size), nil
	}
	return 0, fmt.Errorf("size cache miss")
}
