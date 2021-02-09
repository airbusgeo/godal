package main

import "C"

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/airbusgeo/godal/gcs"
)

var ctx context.Context

func blockSize() int {
	s := os.Getenv("GODAL_BLOCKSIZE")
	const (
		BYTE = 1 << (10 * iota)
		KILOBYTE
		MEGABYTE
		GIGABYTE
		TERABYTE
		PETABYTE
		EXABYTE
	)
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0
	}
	s = strings.ToUpper(s)

	i := strings.IndexFunc(s, unicode.IsLetter)

	if i == -1 {
		ii, err := strconv.Atoi(s)
		if err != nil || ii <= 0 {
			log.Printf("failed to parse GODAL_BLOCKSIZE %s", s)
			return 0
		}
		return ii
	}

	bytesString, multiple := s[:i], s[i:]
	bytes, err := strconv.ParseFloat(bytesString, 64)
	if err != nil || bytes < 0 {
		log.Printf("failed to parse GODAL_BLOCKSIZE %s", s)
		return 0
	}

	switch multiple {
	case "E", "EB", "EIB":
		return int(bytes * EXABYTE)
	case "P", "PB", "PIB":
		return int(bytes * PETABYTE)
	case "T", "TB", "TIB":
		return int(bytes * TERABYTE)
	case "G", "GB", "GIB":
		return int(bytes * GIGABYTE)
	case "M", "MB", "MIB":
		return int(bytes * MEGABYTE)
	case "K", "KB", "KIB":
		return int(bytes * KILOBYTE)
	case "B":
		return int(bytes)
	default:
		log.Printf("failed to parse GODAL_BLOCKSIZE %s", s)
		return 0
	}
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
	opts := []gcs.Option{}
	if bs := blockSize(); bs > 0 {
		opts = append(opts, gcs.BlockSize(bs))
	}
	if nb := numBlocks(); nb > 0 {
		opts = append(opts, gcs.MaxCachedBlocks(nb))
	}
	if splitRanges() {
		opts = append(opts, gcs.SplitConsecutiveRanges(true))
	}
	err := gcs.RegisterHandler(ctx, opts...)
	if err != nil {
		log.Printf("Failed to register gcs handler: %v", err)
		return
	}
	go func() {
		<-ctx.Done()
	}()
	return
}

func main() {}
