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
	//CSV comma-separated values driver
	CSV DriverName = "CSV"
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
	CSV: {
		vectorName:     "CSV",
		vectorRegister: "RegisterOGRCSV",
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

type driversOpt struct {
	drivers []string
}

//Drivers specifies the list of drivers that are allowed to try opening the dataset
func Drivers(drivers ...string) interface {
	OpenOption
} {
	return driversOpt{drivers}
}
func (do driversOpt) setOpenOpt(oo *openOpts) {
	oo.drivers = append(oo.drivers, do.drivers...)
}

type driverOpenOption struct {
	oo []string
}

//DriverOpenOption adds a list of Open Options (-oo switch) to the open command. Each keyval must
//be provided in a "KEY=value" format
func DriverOpenOption(keyval ...string) interface {
	OpenOption
	BuildVRTOption
} {
	return driverOpenOption{keyval}
}
func (doo driverOpenOption) setOpenOpt(oo *openOpts) {
	oo.options = append(oo.options, doo.oo...)
}
func (doo driverOpenOption) setBuildVRTOpt(bvo *buildVRTOpts) {
	bvo.openOptions = append(bvo.openOptions, doo.oo...)
}
