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
