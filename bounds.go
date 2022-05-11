package godal

import "math"

// Bounds represents an envelope in the order minx,miny,maxx,maxy
type Bounds [4]float64

func (b Bounds) MinX() float64 {
	return b[0]
}

func (b Bounds) MinY() float64 {
	return b[1]
}

func (b Bounds) MaxX() float64 {
	return b[2]
}

func (b Bounds) MaxY() float64 {
	return b[3]
}

// Union returns the union of these bounds with other ones
func (b Bounds) Union(other Bounds) Bounds {
	return [4]float64{
		math.Min(b.MinX(), other.MinX()),
		math.Min(b.MinY(), other.MinY()),
		math.Max(b.MaxX(), other.MaxX()),
		math.Max(b.MaxY(), other.MaxY()),
	}
}
