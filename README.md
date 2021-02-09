# Golang bindings for GDAL
[![Go Reference](https://pkg.go.dev/badge/github.com/airbusgeo/godal.svg)](https://pkg.go.dev/github.com/airbusgeo/godal)
[![License](https://img.shields.io/github/license/airbusgeo/godal.svg)](https://github.com/airbusgeo/godal/blob/main/LICENSE)
[![made-for-GDAL](https://img.shields.io/badge/Made%20for-GDAL-71c9f1.svg)](https://gdal.org)
[![Build Status](https://github.com/airbusgeo/godal/workflows/build/badge.svg?branch=main&event=push)](https://github.com/airbusgeo/godal/actions?query=workflow%3Agodal+event%3Apush+branch%3Amain)
[![Coverage Status](https://coveralls.io/repos/github/airbusgeo/godal/badge.svg?branch=main)](https://coveralls.io/github/airbusgeo/godal?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/airbusgeo/godal)](https://goreportcard.com/report/github.com/airbusgeo/godal)



### Goals

Godal aims at providing an idiomatic go wrapper around the <img src="https://gdal.org/_static/gdalicon.png" width="25" height="25">
[GDAL](https://gdal.org) library:

* Function calls return a result and an error. The result will be valid if
  no error was returned. The error message will contain the root cause of why
  the error happened.
* Calls between go and native libraries incur some overhead. As such godal does
  not strictly expose GDAL's API, but groups often-used calls in a single cgo function
  to reduce this overhead. For example, c++ code like
```c++
    hDS = GDALOpen(filename, GA_Readonly)
    if (hDS == NULL) exit(1);
    int sx = GDALGetRasterXSize(hDS);
    int sy = GDALGetRasterYSize(hDS);
    int nBands = GDALGetRasterCount(hDS);
    printf("dataset size: %dx%dx%d\n",sx,sy,nBands);
    for (int i=1; i<=nBands; i++) {
        hBand = GDALGetRasterBand(hDS,i);
        int ovrCount = GDALGetOverviewCount(hBand)
        for(int o=0; o<=ovrCount; o++) {
            GDALRasterBandH oBand = GDALGetOverview(hBand,o);
            int osx = GDALGetRasterBandXSize(oBand);
            int osy = GDALGetRasterBandYSize(oBand);
            printf("overview %d size: %dx%d\n",o,osx,osy);
        }
    }
```
will be written as
```go
    hDS,err := godal.Open(filename)
    if err!=nil {
        panic(err)
    }
    structure := hDS.Structure()
    fmt.Printf("dataset size: %dx%dx%d\n", shape.SizeX,shape.SizeY,shape.NBands)
    for _,band := range hDS.Bands() {
        for o,ovr := range band.Overviews() {
            bstruct := ovr.Structure()
            fmt.Printf("overview %d size: %dx%d\n",o,structure.SizeX,structure.SizeY)
        }
    }
```
* Unfrequently used or non-default parameters are passed as options:
```go
    ds,err := godal.Open(filename) //read-only
    ds,err := godal.Open(filename, Update()) //read-write
```
* Godal exposes a VSI handler that can easily allow you to expose any
  [io.ReaderAt](https://golang.org/pkg/io/#ReaderAt) as a filename that can be
  opened by GDAL. A handler for opening `gs://` google cloud storage URIs is
  provided.

### Documentation

[![GoReference](https://pkg.go.dev/badge/github.com/airbusgeo/godal.svg)](https://pkg.go.dev/github.com/airbusgeo/godal)
contains the API reference and example code to get you started. The
`*_test.go` files can also be used as reference.


### Status

Godal is not feature complete. The raster side is nearing completion and
should remain stable. The vector and spatial-referencing sides are far from
complete, meaning that the API might evolve in backwards incompatible ways
until essential functionality is covered.

### Contributing

Contributions are welcome. Please read the [contribution guidelines](https://github.com/airbusgeo/godal/blob/main/.github/CONTRIBUTING.md)
before submitting fixes or enhancements.

### Installation

Godal requires a GDAL version greater than 3.2. Make sure the GDAL headers
are installed on the system used for compiling go+godal code. If using a GDAL
installation in a non standard location, you can set your `PKG_CONFIG_PATH`
environment variable, e.g. `export PKG_CONFIG_PATH=/opt/include/pkgconfig`.

### Licensing
Godal is licensed under the Apache License, Version 2.0. See
[LICENSE](https://github.com/airbusgeo/godal/blob/main/LICENSE) for the full
license text.


