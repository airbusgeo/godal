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
	"github.com/airbusgeo/osio/s3"
)

var ctx context.Context

func blockSize() string {
	s := strings.TrimSpace(os.Getenv("GODAL_BLOCKSIZE"))
	if s == "" {
		return "512k"
	}
	return s
}

func numBlocks() int {
	s := os.Getenv("GODAL_NUMBLOCKS")
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 1024
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
//export GDALRegister_s3
func GDALRegister_s3() {
	ctx = context.Background()
	opts := []osio.AdapterOption{}
	bs := ""
	nb := 0
	if bs = blockSize(); bs != "" {
		opts = append(opts, osio.BlockSize(bs))
	}
	if nb = numBlocks(); nb > 0 {
		opts = append(opts, osio.NumCachedBlocks(nb))
	}
	gcsh, err := s3.Handle(ctx)
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
	err = godal.RegisterVSIHandler("s3://", gcsa)
	if err != nil {
		log.Printf("godal.registervsiadapter() failed: %v", err)
		return
	}
	go func() {
		<-ctx.Done()
	}()
}

func main() {}
