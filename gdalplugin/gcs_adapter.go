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

func splitRanges() bool {
	s := strings.ToLower(strings.TrimSpace(os.Getenv("GODAL_SPLIT_CONSECUTIVE_RANGES")))
	switch s {
	case "", "0", "no", "false":
		return false
	default:
		return true
	}
}

//export GDALRegister_gcs
func GDALRegister_gcs() {
	ctx = context.Background()
	opts := []osio.AdapterOption{
		osio.SplitRanges(splitRanges()),
	}
	if bs := blockSize(); bs != "" {
		opts = append(opts, osio.BlockSize(bs))
	}
	if nb := numBlocks(); nb > 0 {
		opts = append(opts, osio.NumCachedBlocks(nb))
	}
	gcs, err := osio.GCSHandle(ctx)
	if err != nil {
		log.Printf("osio.gcshandle() failed: %v", err)
		return
	}
	gcsa, err := osio.NewAdapter(gcs, opts...)
	if err != nil {
		log.Printf("osio.newadapter() failed: %v", err)
		return
	}
	err = godal.RegisterVSIAdapter("gs://", gcsa)
	if err != nil {
		log.Printf("godal.registervsiadapter() failed: %v", err)
		return
	}
	go func() {
		<-ctx.Done()
	}()
}

func main() {}
