package godal

import "fmt"

type srWKTOpts struct {
	errorHandler ErrorHandler
}

//WKTExportOption is an option that can be passed to SpatialRef.WKT()
//
// Available WKTExportOptions are:
//
// • ErrLogger
type WKTExportOption interface {
	setWKTExportOpt(sro *srWKTOpts)
}

type trnOpts struct {
	errorHandler ErrorHandler
}

// TransformOption is an option that can be passed to NewTransform
//
// Available TransformOptions are:
//
// • ErrLogger
type TransformOption interface {
	setTransformOpt(o *trnOpts)
}

func (sr *SpatialRef) setBoundsOpt(o *boundsOpts) {
	o.sr = sr
}

type boundsOpts struct {
	sr *SpatialRef
	//TODO: errorHandler ErrorHandler
}

// BoundsOption is an option that can be passed to Dataset.Bounds or Geometry.Bounds
//
// Available options are:
//
// • *SpatialRef
//
// • TODO: ErrLogger
type BoundsOption interface {
	setBoundsOpt(o *boundsOpts)
}

type createSpatialRefOpts struct {
	errorHandler ErrorHandler
}

type CreateSpatialRefOption interface {
	setCreateSpatialRefOpt(so *createSpatialRefOpts)
}

func reprojectBounds(bnds [4]float64, src, dst *SpatialRef) ([4]float64, error) {
	var ret [4]float64
	trn, err := NewTransform(src, dst)
	if err != nil {
		return ret, fmt.Errorf("create coordinate transform: %w", err)
	}
	defer trn.Close()
	x := []float64{bnds[0], bnds[0], bnds[2], bnds[2]}
	y := []float64{bnds[1], bnds[3], bnds[3], bnds[1]}
	err = trn.TransformEx(x, y, nil, nil)
	if err != nil {
		return ret, fmt.Errorf("reproject bounds: %w", err)
	}
	ret[0] = x[0]
	ret[1] = y[0]
	ret[2] = x[0]
	ret[3] = y[0]
	for i := 1; i < 4; i++ {
		if x[i] < ret[0] {
			ret[0] = x[i]
		}
		if x[i] > ret[2] {
			ret[2] = x[i]
		}
		if y[i] < ret[1] {
			ret[1] = y[i]
		}
		if y[i] > ret[3] {
			ret[3] = y[i]
		}
	}
	return ret, nil
}
