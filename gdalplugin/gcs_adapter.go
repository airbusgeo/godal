package main

import "C"

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/airbusgeo/godal"
	"github.com/airbusgeo/osio"
	"github.com/airbusgeo/osio/gcs"
)

var ctx context.Context

func blockSize() string {
	return os.Getenv("GODAL_BLOCKSIZE")
}

func numBlocks() int {
	s := os.Getenv("GODAL_NUMBLOCKS")
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 64
	}
	ii, err := strconv.Atoi(s)
	if err != nil || ii <= 0 {
		log.Printf("failed to parse GODAL_NUMBLOCKS %s", s)
		return 0
	}
	return ii
}

// GDALRegister_gcs is called by gdal when loading this so. It is not meant to be used directly from go.
//
//export GDALRegister_gcs
func GDALRegister_gcs() {
	ctx = context.Background()
	opts := []osio.AdapterOption{}
	if bs := blockSize(); bs != "" {
		opts = append(opts, osio.BlockSize(bs))
	}
	if nb := numBlocks(); nb > 0 {
		opts = append(opts, osio.NumCachedBlocks(nb))
	}
	gcsh, err := gcs.Handle(ctx)
	if err != nil {
		log.Printf("osio.gcshandle() failed: %v", err)
		return
	}
	sLog := os.Getenv("GODAL_LOG")
	if sLog != "" && strings.ToUpper(os.Getenv("GODAL_LOG")) != "FALSE" {
		opts = append(opts, osio.WithLogger(osio.StdLogger))
	}
	gcsa, err := osio.NewAdapter(gcsh, opts...)
	if err != nil {
		log.Printf("osio.newadapter() failed: %v", err)
		return
	}
	err = godal.RegisterVSIHandler("gs://", gcsa)
	if err != nil {
		log.Printf("godal.registervsiadapter() failed: %v", err)
		return
	}
	go func() {
		<-ctx.Done()
	}()
}

func main() {}
