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

//#include "godal.h"
import "C"
import (
	"errors"
	"fmt"
	"strconv"
	"unsafe"
)

// GeometryType is a geometry type
type GeometryType uint32

const (
	//GTUnknown is a GeomtryType
	GTUnknown = GeometryType(C.wkbUnknown)
	//GTPoint is a GeomtryType
	GTPoint = GeometryType(C.wkbPoint)
	//GTLineString is a GeomtryType
	GTLineString = GeometryType(C.wkbLineString)
	//GTPolygon is a GeomtryType
	GTPolygon = GeometryType(C.wkbPolygon)
	//GTMultiPoint is a GeomtryType
	GTMultiPoint = GeometryType(C.wkbMultiPoint)
	//GTMultiLineString is a GeomtryType
	GTMultiLineString = GeometryType(C.wkbMultiLineString)
	//GTMultiPolygon is a GeomtryType
	GTMultiPolygon = GeometryType(C.wkbMultiPolygon)
	//GTGeometryCollection is a GeomtryType
	GTGeometryCollection = GeometryType(C.wkbGeometryCollection)
	//GTNone is a GeomtryType
	GTNone = GeometryType(C.wkbNone)
)

//FieldType is a vector field (attribute/column) type
type FieldType C.OGRFieldType

const (
	//FTInt is a Simple 32bit integer.
	FTInt = FieldType(C.OFTInteger)
	//FTReal is a Double Precision floating point.
	FTReal = FieldType(C.OFTReal)
	//FTString is a String of ASCII chars.
	FTString = FieldType(C.OFTString)
	//FTInt64 is a Single 64bit integer.
	FTInt64 = FieldType(C.OFTInteger64)
	//FTIntList is a List of 32bit integers.
	FTIntList = FieldType(C.OFTIntegerList)
	//FTRealList is a List of doubles.
	FTRealList = FieldType(C.OFTRealList)
	//FTStringList is a Array of strings.
	FTStringList = FieldType(C.OFTStringList)
	//FTBinary is a Raw Binary data.
	FTBinary = FieldType(C.OFTBinary)
	//FTDate is a Date.
	FTDate = FieldType(C.OFTDate)
	//FTTime is a Time.
	FTTime = FieldType(C.OFTTime)
	//FTDateTime is a Date and Time.
	FTDateTime = FieldType(C.OFTDateTime)
	//FTInt64List is a List of 64bit integers.
	FTInt64List = FieldType(C.OFTInteger64List)
)

//FieldDefinition defines a single attribute
type FieldDefinition struct {
	name  string
	ftype FieldType
}

//NewFieldDefinition creates a FieldDefinition
func NewFieldDefinition(name string, fdtype FieldType) *FieldDefinition {
	return &FieldDefinition{
		name:  name,
		ftype: fdtype,
	}
}

func (fd *FieldDefinition) setCreateLayerOpt(o *createLayerOpts) {
	o.fields = append(o.fields, fd)
}

func (fd *FieldDefinition) createHandle() C.OGRFieldDefnH {
	cfname := unsafe.Pointer(C.CString(fd.name))
	defer C.free(cfname)
	cfd := C.OGR_Fld_Create((*C.char)(cfname), C.OGRFieldType(fd.ftype))
	return cfd
}

type dsVectorTranslateOpts struct {
	config   []string
	creation []string
	driver   DriverName
}

// DatasetVectorTranslateOption is an option that can be passed to Dataset.Warp()
//
// Available Options are:
//
// • CreationOption
// • ConfigOption
// • DriverName
type DatasetVectorTranslateOption interface {
	setDatasetVectorTranslateOpt(dwo *dsVectorTranslateOpts)
}

// VectorTranslate runs the library version of ogr2ogr
// See the ogr2ogr doc page to determine the valid flags/opts that can be set in switches.
//
// Example switches :
//  []string{
//    "-f", "GeoJSON",
//	  "-t_srs","epsg:3857",
//    "-dstalpha"}
//
// Creation options and Driver may be set either in the switches slice with
//  switches:=[]string{"-dsco","TILED=YES", "-f","GeoJSON"}
// or through Options with
//  ds.VectorTranslate(dst, switches, CreationOption("TILED=YES","BLOCKXSIZE=256"), GeoJSON)
func (ds *Dataset) VectorTranslate(dstDS string, switches []string, opts ...DatasetVectorTranslateOption) (*Dataset, error) {
	gopts := dsVectorTranslateOpts{}
	for _, opt := range opts {
		opt.setDatasetVectorTranslateOpt(&gopts)
	}
	for _, copt := range gopts.creation {
		switches = append(switches, "-dsco", copt)
	}
	if gopts.driver != "" {
		dname := string(gopts.driver)
		if dm, ok := driverMappings[gopts.driver]; ok {
			dname = dm.vectorName
		}
		switches = append(switches, "-f", dname)
	}
	cswitches := sliceToCStringArray(switches)
	defer cswitches.free()
	cconfig := sliceToCStringArray(gopts.config)
	defer cconfig.free()

	cname := unsafe.Pointer(C.CString(dstDS))
	defer C.free(cname)

	var errmsg *C.char
	hndl := C.godalDatasetVectorTranslate((*C.char)(cname), ds.Handle(), cswitches.cPointer(), &errmsg, cconfig.cPointer())
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Dataset{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// Layer wraps an OGRLayerH
type Layer struct {
	majorObject
}

// Handle returns a pointer to the underlying GDALRasterBandH
func (layer Layer) Handle() C.OGRLayerH {
	return C.OGRLayerH(layer.majorObject.handle)
}

// FeatureCount returns the number of features in the layer
func (layer Layer) FeatureCount() (int, error) {
	var count C.int
	errmsg := C.godalLayerFeatureCount(layer.Handle(), &count)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return 0, errors.New(C.GoString(errmsg))
	}
	return int(count), nil
}

// Layers returns all dataset layers
func (ds *Dataset) Layers() []Layer {
	clayers := C.godalVectorLayers(ds.Handle())
	if clayers == nil {
		return nil
	}
	defer C.free(unsafe.Pointer(clayers))
	//https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
	sLayers := (*[1 << 30]C.OGRLayerH)(unsafe.Pointer(clayers))
	layers := []Layer{}
	i := 0
	for {
		if sLayers[i] == nil {
			return layers
		}
		layers = append(layers, Layer{majorObject{C.GDALMajorObjectH(sLayers[i])}})
		i++
	}
}

// SpatialRef returns dataset projection.
func (layer Layer) SpatialRef() *SpatialRef {
	hndl := C.OGR_L_GetSpatialRef(layer.Handle())
	return &SpatialRef{handle: hndl, isOwned: false}
}

// Geometry wraps a OGRGeometryH
type Geometry struct {
	isOwned bool
	handle  C.OGRGeometryH
}

//Simplify simplifies the geometry with the given tolerance
func (g *Geometry) Simplify(tolerance float64) (*Geometry, error) {
	var errmsg *C.char
	hndl := C.godal_OGR_G_Simplify(g.handle, C.double(tolerance), &errmsg)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}, nil
}

//Buffer buffers the geometry
func (g *Geometry) Buffer(distance float64, segments int) (*Geometry, error) {
	var errmsg *C.char
	hndl := C.godal_OGR_G_Buffer(g.handle, C.double(distance), C.int(segments), &errmsg)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Geometry{
		isOwned: true,
		handle:  hndl,
	}, nil
}

//Empty retruens wether the underlying geometry is empty
func (g *Geometry) Empty() bool {
	e := C.OGR_G_IsEmpty(g.handle)
	return e != 0
}

//Bounds returns the geometry's envelope in the order minx,miny,maxx,maxy
func (g *Geometry) Bounds(opts ...BoundsOption) ([4]float64, error) {
	bo := boundsOpt{}
	for _, o := range opts {
		o.setBoundsOpt(&bo)
	}
	var env C.OGREnvelope
	C.OGR_G_GetEnvelope(g.handle, &env)
	bnds := [4]float64{
		float64(env.MinX),
		float64(env.MinY),
		float64(env.MaxX),
		float64(env.MaxY),
	}
	if bo.sr == nil {
		return bnds, nil
	}
	sr := g.SpatialRef()
	defer sr.Close()
	ret, err := reprojectBounds(bnds, sr, bo.sr)
	if err != nil {
		return bnds, err
	}
	return ret, nil
}

// Close may reclaim memory from geometry. Must be called exactly once.
func (g *Geometry) Close() {
	if g.handle == nil {
		return
		//panic("geometry already closed")
	}
	if g.isOwned {
		C.OGR_G_DestroyGeometry(g.handle)
	}
	g.handle = nil
}

//Feature is a Layer feature
type Feature struct {
	handle C.OGRFeatureH
}

//Geometry returns a handle to the feature's geometry
func (f *Feature) Geometry() *Geometry {
	hndl := C.OGR_F_GetGeometryRef(f.handle)
	return &Geometry{
		isOwned: false,
		handle:  hndl,
	}
}

//SetGeometry overwrites the feature's geometry
func (f *Feature) SetGeometry(geom *Geometry) error {
	errmsg := C.godalFeatureSetGeometry(f.handle, geom.handle)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

//Field is a Feature attribute
type Field struct {
	ftype    FieldType
	intVal   int64
	floatVal float64
	strVal   string
	//byteVal  []byte
}

//Type returns the field's native type
func (fld Field) Type() FieldType {
	return fld.ftype
}

//Int returns the Field as an integer
func (fld Field) Int() int64 {
	switch fld.ftype {
	case FTInt, FTInt64:
		return fld.intVal
	case FTReal:
		return int64(fld.floatVal)
	case FTString:
		ii, _ := strconv.Atoi(fld.strVal)
		return int64(ii)
	default:
		return 0
	}
}

//Float returns the field as a float64
func (fld Field) Float() float64 {
	switch fld.ftype {
	case FTInt, FTInt64:
		return float64(fld.intVal)
	case FTReal:
		return fld.floatVal
	case FTString:
		ii, _ := strconv.ParseFloat(fld.strVal, 64)
		return ii
	default:
		return 0
	}
}

//String returns the field as a string
func (fld Field) String() string {
	switch fld.ftype {
	case FTInt, FTInt64:
		return fmt.Sprintf("%d", fld.intVal)
	case FTReal:
		return fmt.Sprintf("%f", fld.floatVal)
	case FTString:
		return fld.strVal
	default:
		return ""
	}
}

//Fields returns all the Feature's fields
func (f *Feature) Fields() map[string]Field {
	fcount := C.OGR_F_GetFieldCount(f.handle)
	if fcount == 0 {
		return nil
	}
	retm := make(map[string]Field)
	for fid := C.int(0); fid < fcount; fid++ {
		fdefn := C.OGR_F_GetFieldDefnRef(f.handle, fid)
		fname := C.GoString(C.OGR_Fld_GetNameRef(fdefn))
		ftype := C.OGR_Fld_GetType(fdefn)
		fld := Field{}
		switch ftype {
		case C.OFTInteger:
			fld.ftype = FTInt
			fld.intVal = int64(C.OGR_F_GetFieldAsInteger64(f.handle, fid))
		case C.OFTInteger64:
			fld.ftype = FTInt64
			fld.intVal = int64(C.OGR_F_GetFieldAsInteger64(f.handle, fid))
		case C.OFTReal:
			fld.ftype = FTReal
			fld.floatVal = float64(C.OGR_F_GetFieldAsDouble(f.handle, fid))
		case C.OFTString:
			fld.ftype = FTString
			fld.strVal = C.GoString(C.OGR_F_GetFieldAsString(f.handle, fid))
			/*
				case C.OFTBinary:
					fallthrough
					//fld.ftype = FTBinary
					//var ll C.int
					//cdata := C.OGR_F_GetFieldAsBinary(f.handle, fid, &ll)
					//fld.byteVal = C.GoBytes(unsafe.Pointer(cdata), ll)
				case C.OFTDate:
					fallthrough
				case C.OFTTime:
					fallthrough
				case C.OFTDateTime:
					fallthrough
				case C.OFTInteger64List:
					fallthrough
				case C.OFTIntegerList:
					fallthrough
				case C.OFTStringList:
					fallthrough
				case C.OFTRealList:
					fallthrough
			*/
		default:
			//TODO
			continue
		}
		retm[fname] = fld
	}
	return retm
}

//Close releases resources associated to a feature
func (f *Feature) Close() {
	if f.handle == nil {
		return
		//panic("feature closed more than once")
	}
	C.OGR_F_Destroy(f.handle)
	f.handle = nil
}

// ResetReading makes Layer.NextFeature return the first feature of the layer
func (layer Layer) ResetReading() {
	C.OGR_L_ResetReading(layer.Handle())
}

// NextFeature returns the layer's next feature, or nil if there are no mo features
func (layer Layer) NextFeature() *Feature {
	hndl := C.OGR_L_GetNextFeature(layer.Handle())
	if hndl == nil {
		return nil
	}
	return &Feature{hndl}
}

type newFeatureOpt struct{}

//NewFeatureOption is an option that can be passed to Layer.NewFeature
//
// Available options are:
//
// • none yet
type NewFeatureOption interface {
	setNewFeatureOpt(nfo *newFeatureOpt)
}

// NewFeature creates a feature on Layer
func (layer Layer) NewFeature(geom *Geometry, opts ...NewFeatureOption) (*Feature, error) {
	nfo := newFeatureOpt{}
	for _, opt := range opts {
		opt.setNewFeatureOpt(&nfo)
	}
	var errmsg *C.char
	ghandle := C.OGRGeometryH(nil)
	if geom != nil {
		ghandle = geom.handle
	}
	hndl := C.godalLayerNewFeature(layer.Handle(), ghandle, (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Feature{hndl}, nil
}

// UpdateFeature rewrites an updated feature in the Layer
func (layer Layer) UpdateFeature(feat *Feature) error {
	errmsg := C.godalLayerSetFeature(layer.Handle(), feat.handle)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// DeleteFeature deletes feature from the Layer.
func (layer Layer) DeleteFeature(feat *Feature) error {
	errmsg := C.godalLayerDeleteFeature(layer.Handle(), feat.handle)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

type createLayerOpts struct {
	fields []*FieldDefinition
}

// CreateLayerOption is an option that can be passed to Dataset.CreateLayer()
type CreateLayerOption interface {
	setCreateLayerOpt(clo *createLayerOpts)
}

// CreateLayer creates a new vector layer
//
// Available CreateLayerOptions are
//
// • FieldDefinition (may be used multiple times) to add attribute fields to the layer
func (ds *Dataset) CreateLayer(name string, sr *SpatialRef, gtype GeometryType, opts ...CreateLayerOption) (Layer, error) {
	co := createLayerOpts{}
	for _, opt := range opts {
		opt.setCreateLayerOpt(&co)
	}
	srHandle := C.OGRSpatialReferenceH(nil)
	if sr != nil {
		srHandle = sr.handle
	}
	var errmsg *C.char
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	hndl := C.godalCreateLayer(ds.Handle(), (*C.char)(unsafe.Pointer(cname)), srHandle, C.OGRwkbGeometryType(gtype), (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return Layer{}, errors.New(C.GoString(errmsg))
	}
	if len(co.fields) > 0 {
		for _, fld := range co.fields {
			fhndl := fld.createHandle()
			//TODO error checking
			C.OGR_L_CreateField(hndl, fhndl, C.int(0))
			C.OGR_Fld_Destroy(fhndl)
		}
	}
	return Layer{majorObject{C.GDALMajorObjectH(hndl)}}, nil
}

// NewGeometryFromWKT creates a new Geometry from its WKT representation
func NewGeometryFromWKT(wkt string, sr *SpatialRef) (*Geometry, error) {
	srHandle := C.OGRSpatialReferenceH(nil)
	if sr != nil {
		srHandle = sr.handle
	}
	var errmsg *C.char
	cwkt := C.CString(wkt)
	defer C.free(unsafe.Pointer(cwkt))
	hndl := C.godalNewGeometryFromWKT((*C.char)(unsafe.Pointer(cwkt)), srHandle, (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Geometry{isOwned: true, handle: hndl}, nil
}

// NewGeometryFromWKB creates a new Geometry from its WKB representation
func NewGeometryFromWKB(wkb []byte, sr *SpatialRef) (*Geometry, error) {
	srHandle := C.OGRSpatialReferenceH(nil)
	if sr != nil {
		srHandle = sr.handle
	}
	var errmsg *C.char
	hndl := C.godalNewGeometryFromWKB(unsafe.Pointer(&wkb[0]), C.int(len(wkb)), srHandle, (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	return &Geometry{isOwned: true, handle: hndl}, nil
}

//WKT returns the Geomtry's WKT representation
func (g *Geometry) WKT() (string, error) {
	var errmsg *C.char
	cwkt := C.godalExportGeometryWKT(g.handle, (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return "", errors.New(C.GoString(errmsg))
	}
	wkt := C.GoString(cwkt)
	C.CPLFree(unsafe.Pointer(cwkt))
	return wkt, nil
}

//WKB returns the Geomtry's WKB representation
func (g *Geometry) WKB() ([]byte, error) {
	var cwkb unsafe.Pointer
	clen := C.int(0)
	errmsg := C.godalExportGeometryWKB(&cwkb, &clen, g.handle)
	/* wkb export never errors
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return nil, errors.New(C.GoString(errmsg))
	}
	*/
	_ = errmsg
	wkb := C.GoBytes(unsafe.Pointer(cwkb), clen)
	C.free(unsafe.Pointer(cwkb))
	return wkb, nil
}

// SpatialRef returns the geometry's SpatialRef
func (g *Geometry) SpatialRef() *SpatialRef {
	hndl := C.OGR_G_GetSpatialReference(g.handle)
	return &SpatialRef{
		handle:  hndl,
		isOwned: false,
	}
}

// SetSpatialRef assigns the given SpatialRef to the Geometry. It does not
// perform an actual reprojection.
func (g *Geometry) SetSpatialRef(sr *SpatialRef) {
	C.OGR_G_AssignSpatialReference(g.handle, sr.handle)
}

// Reproject reprojects the given geometry to the given SpatialRef
func (g *Geometry) Reproject(to *SpatialRef) error {
	errmsg := C.godalGeometryTransformTo(g.handle, to.handle)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

// Transform transforms the given geometry. g is expected to already be
// in the supplied Transform source SpatialRef.
func (g *Geometry) Transform(trn *Transform) error {
	errmsg := C.godalGeometryTransform(g.handle, trn.handle, trn.dst)
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return errors.New(C.GoString(errmsg))
	}
	return nil
}

type geojsonOpt struct {
	precision C.int
}

//GeoJSONOption is an option that can be passed to Geometry.GeoJSON
type GeoJSONOption interface {
	setGeojsonOpt(gjo *geojsonOpt)
}

type significantDigits int

func (sd significantDigits) setGeojsonOpt(o *geojsonOpt) {
	o.precision = C.int(sd)
}

// SignificantDigits sets the number of significant digits after the decimal separator should
// be kept for geojson output
func SignificantDigits(n int) interface {
	GeoJSONOption
} {
	return significantDigits(n)
}

// GeoJSON returns the geometry in geojson format. The geometry is expected to be in epsg:4326
// projection per RFCxxx
//
// Available GeoJSONOptions are
//
// • SignificantDigits(n int) to keep n significant digits after the decimal separator (default: 8)
func (g *Geometry) GeoJSON(opts ...GeoJSONOption) (string, error) {
	gjo := geojsonOpt{
		precision: 7,
	}
	for _, opt := range opts {
		opt.setGeojsonOpt(&gjo)
	}
	var errmsg *C.char
	gjdata := C.godalExportGeometryGeoJSON(g.handle, gjo.precision, (**C.char)(unsafe.Pointer(&errmsg)))
	if errmsg != nil {
		defer C.free(unsafe.Pointer(errmsg))
		return "", errors.New(C.GoString(errmsg))
	}
	wkt := C.GoString(gjdata)
	C.CPLFree(unsafe.Pointer(gjdata))
	return wkt, nil

}
