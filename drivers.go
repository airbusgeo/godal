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

/*
#include "godal.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

//DriverName is GDAL driver
type DriverName string

const (
	//GTiff GeoTIFF
	GTiff DriverName = "GTiff"
	//GeoJSON RFCxxxx geojson
	GeoJSON DriverName = "GeoJSON"
	//Memory in memory driver
	Memory DriverName = "Memory"
	//VRT is a VRT
	VRT DriverName = "VRT"
	//Shapefile is an ESRI Shapefile
	Shapefile DriverName = "ESRI Shapefile"
	//GeoPackage is a geo-package
	GeoPackage DriverName = "GPKG"
	//JP2KAK is a Kakadu Jpeg2000
	JP2KAK DriverName = "JP2KAK"
	//OpenJPEG is an OpenJPEG JPEG2000
	OpenJPEG DriverName = "OpenJPEG"
	//DIMAP is a Dimap
	DIMAP DriverName = "DIMAP"
	//HFA is an erdas img
	HFA DriverName = "HFA"
	//Mitab is a mapinfo mif/tab file
	Mitab DriverName = "Mitab"
)

type driverMapping struct {
	rasterName     string
	vectorName     string
	rasterRegister string
	vectorRegister string
}

var driverMappings = map[DriverName]driverMapping{
	GTiff: {
		rasterName:     "GTiff",
		rasterRegister: "GDALRegister_GTiff",
	},
	Memory: {
		rasterName:     "MEM",
		vectorName:     "Memory",
		rasterRegister: "GDALRegister_MEM",
		vectorRegister: "RegisterOGRMEM",
	},
	GeoJSON: {
		vectorName:     "GeoJSON",
		vectorRegister: "RegisterOGRGeoJSON",
	},
	VRT: {
		rasterName:     "VRT",
		vectorName:     "OGR_VRT",
		rasterRegister: "GDALRegister_VRT",
		vectorRegister: "RegisterOGRVRT",
	},
	Shapefile: {
		vectorName:     "ESRI Shapefile",
		vectorRegister: "RegisterOGRShape",
	},
	GeoPackage: {
		rasterName:     "GPKG",
		vectorName:     "GPKG",
		rasterRegister: "RegisterOGRGeoPackage",
		vectorRegister: "RegisterOGRGeoPackage",
	},
	JP2KAK: {
		rasterName:     "JP2KAK",
		rasterRegister: "GDALRegister_JP2KAK",
	},
	OpenJPEG: {
		rasterName:     "OpenJPEG",
		rasterRegister: "GDALRegister_JP2OpenJPEG",
	},
	DIMAP: {
		rasterName:     "DIMAP",
		rasterRegister: "GDALRegister_DIMAP",
	},
	HFA: {
		rasterName:     "HFA",
		rasterRegister: "GDALRegister_HFA",
	},
	Mitab: {
		vectorName:     "Mapinfo File",
		vectorRegister: "RegisterOGRTAB",
	},
}

func (dn DriverName) setDatasetVectorTranslateOpt(to *dsVectorTranslateOpts) {
	to.driver = dn
}

func (dn DriverName) setDatasetTranslateOpt(to *dsTranslateOpts) {
	to.driver = dn
}

func (dn DriverName) setDatasetWarpOpt(to *dsWarpOpts) {
	to.driver = dn
}

func (dn DriverName) setRasterizeOpt(to *rasterizeOpts) {
	to.driver = dn
}

//compile time checks
var _ DatasetVectorTranslateOption = DriverName("")
var _ DatasetTranslateOption = DriverName("")
var _ DatasetWarpOption = DriverName("")
var _ RasterizeOption = DriverName("")

//RegisterAll calls GDALAllRegister
func RegisterAll() {
	C.GDALAllRegister()
}

// RegisterRaster registers a raster driver by name.
//
// Calling RegisterRaster(DriverName) with one of the predefined DriverNames provided by the library will
// register the corresponding raster driver.
//
// Calling RegisterRaster(DriverName("XXX")) with "XXX" any string will result in calling the function
// GDALRegister_XXX() if it could be found inside the ligdal.so binary. This allows to register any raster driver
// known to gdal but not explicitely defined inside this golang wrapper. Note that "XXX" must be provided
// exactly (i.e. respecting uppercase/lowercase) the same as the names of the C functions GDALRegister_XXX()
// that can be found in gdal.h
func RegisterRaster(drivers ...DriverName) error {
	for _, driver := range drivers {
		switch driver {
		case Memory:
			C.GDALRegister_MEM()
		case VRT:
			C.GDALRegister_VRT()
		case HFA:
			C.GDALRegister_HFA()
		case GTiff:
			C.GDALRegister_GTiff()
		default:
			fnname := fmt.Sprintf("GDALRegister_%s", driver)
			drv, ok := driverMappings[driver]
			if ok {
				fnname = drv.rasterRegister
			}
			if fnname == "" {
				return fmt.Errorf("%s driver does not handle rasters", fnname)
			}
			if err := registerDriver(fnname); err != nil {
				return err
			}
		}
	}
	return nil
}

// RegisterVector registers a vector driver by name.
//
// Calling RegisterVector(DriverName) with one of the predefined DriverNames provided by the library will
// register the corresponding vector driver.
//
// Calling RegisterVector(DriverName("XXX")) with "XXX" any string will result in calling the function
// RegisterOGRXXX() if it could be found inside the ligdal.so binary. This allows to register any vector driver
// known to gdal but not explicitely defined inside this golang wrapper. Note that "XXX" must be provided
// exactly (i.e. respecting uppercase/lowercase) the same as the names of the C functions RegisterOGRXXX()
// that can be found in ogrsf_frmts.h
func RegisterVector(drivers ...DriverName) error {
	for _, driver := range drivers {
		switch driver {
		/* TODO: speedup for OGR drivers
		case VRT:
			C.RegisterOGRVRT()
		case Memory:
			C.RegisterOGRMEM()
		case Mitab:
			C.RegisterOGRTAB()
		case GeoJSON:
			C.RegisterOGRGeoJSON()
		*/
		default:
			fnname := fmt.Sprintf("RegisterOGR%s", driver)
			drv, ok := driverMappings[driver]
			if ok {
				fnname = drv.vectorRegister
			}
			if fnname == "" {
				return fmt.Errorf("%s driver does not handle vectors", fnname)
			}
			if err := registerDriver(fnname); err != nil {
				return err
			}
		}
	}
	return nil
}

func registerDriver(fnname string) error {
	cfnname := C.CString(fnname)
	defer C.free(unsafe.Pointer(cfnname))
	ret := C.godalRegisterDriver(cfnname)
	if ret != 0 {
		return fmt.Errorf("failed to call function %s", fnname)
	}
	return nil
}

// RegisterInternalDrivers is a shorthand for registering "essential" gdal/ogr drivers.
//
// It is equivalent to calling RegisterRaster("VRT","MEM","GTiff") and
// RegisterVector("MEM","VRT","GeoJSON")
func RegisterInternalDrivers() {
	//These are always build in and should never error
	_ = RegisterRaster(VRT, Memory, GTiff)
	_ = RegisterVector(VRT, Memory, GeoJSON)
}

// Driver is a gdal format driver
type Driver struct {
	majorObject
}

// Handle returns a pointer to the underlying GDALDriverH
func (drv Driver) Handle() C.GDALDriverH {
	return C.GDALDriverH(drv.majorObject.handle)
}

// VectorDriver returns a Driver by name. It returns false if the named driver does
// not exist
func VectorDriver(name DriverName) (Driver, bool) {
	if dn, ok := driverMappings[name]; ok {
		if dn.vectorName == "" {
			return Driver{}, false
		}
		return getDriver(dn.vectorName)
	}
	return getDriver(string(name))
}

// RasterDriver returns a Driver by name. It returns false if the named driver does
// not exist
func RasterDriver(name DriverName) (Driver, bool) {
	if dn, ok := driverMappings[name]; ok {
		if dn.rasterName == "" {
			return Driver{}, false
		}
		return getDriver(dn.rasterName)
	}
	return getDriver(string(name))
}

func getDriver(name string) (Driver, bool) {
	cname := C.CString(string(name))
	defer C.free(unsafe.Pointer(cname))
	hndl := C.GDALGetDriverByName((*C.char)(unsafe.Pointer(cname)))
	if hndl != nil {
		return Driver{majorObject{C.GDALMajorObjectH(hndl)}}, true
	}
	return Driver{}, false
}

type dsCreateOpts struct {
	config   []string
	creation []string
}

// DatasetCreateOption is an option that can be passed to Create()
//
// Available DatasetCreateOptions are:
//
// • CreationOption
//
// • ConfigOption
type DatasetCreateOption interface {
	setDatasetCreateOpt(dc *dsCreateOpts)
}

// Create wraps GDALCreate and uses driver to creates a new raster dataset with the given name (usually filename), size, type and bands.
func Create(driver DriverName, name string, nBands int, dtype DataType, width, height int, opts ...DatasetCreateOption) (*Dataset, error) {
	drvname := string(driver)
	if drv, ok := driverMappings[driver]; ok {
		if drv.rasterName == "" {
			return nil, fmt.Errorf("%s does not support raster creation", driver)
		}
		drvname = drv.rasterName
	}
	drv, ok := getDriver(drvname)
	if !ok {
		return nil, fmt.Errorf("failed to get driver %s", drvname)
	}
	gopts := dsCreateOpts{}
	for _, opt := range opts {
		opt.setDatasetCreateOpt(&gopts)
	}
	createOpts := sliceToCStringArray(gopts.creation)
	configOpts := sliceToCStringArray(gopts.config)
	cname := C.CString(name)
	defer createOpts.free()
	defer configOpts.free()
	defer C.free(unsafe.Pointer(cname))
	var errmsg *C.char
	hndl := C.godalCreate(drv.Handle(), (*C.char)(unsafe.Pointer(cname)),
		C.int(width), C.int(height), C.int(nBands), C.GDALDataType(dtype),
		createOpts.cPointer(), &errmsg, configOpts.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil

}

// CreateVector wraps GDALCreate and uses driver to create a new vector dataset with the given name
// (usually filename) and options
func CreateVector(driver DriverName, name string, opts ...DatasetCreateOption) (*Dataset, error) {
	drvname := string(driver)
	if drv, ok := driverMappings[driver]; ok {
		if drv.vectorName == "" {
			return nil, fmt.Errorf("%s does not support vector creation", driver)
		}
		drvname = drv.vectorName
	}
	drv, ok := getDriver(drvname)
	if !ok {
		return nil, fmt.Errorf("failed to get driver %s", drvname)
	}
	gopts := dsCreateOpts{}
	for _, opt := range opts {
		opt.setDatasetCreateOpt(&gopts)
	}
	createOpts := sliceToCStringArray(gopts.creation)
	configOpts := sliceToCStringArray(gopts.config)
	cname := C.CString(name)
	defer createOpts.free()
	defer configOpts.free()
	defer C.free(unsafe.Pointer(cname))
	var errmsg *C.char
	hndl := C.godalCreate(drv.Handle(), (*C.char)(unsafe.Pointer(cname)),
		0, 0, 0, C.GDT_Unknown,
		createOpts.cPointer(), &errmsg, configOpts.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil

}
