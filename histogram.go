package godal

// Histogram is a band's histogram.
type Histogram struct {
	min, max float64
	counts   []uint64
}

// Bucket is a histogram entry. It spans [Min,Max] and contains Count entries.
type Bucket struct {
	Min, Max float64
	Count    uint64
}

//Len returns the number of buckets contained in the histogram
func (h Histogram) Len() int {
	return len(h.counts)
}

//Bucket returns the i'th bucket in the histogram. i must be between 0 and Len()-1.
func (h Histogram) Bucket(i int) Bucket {
	width := (h.max - h.min) / float64(len(h.counts))
	return Bucket{
		Min:   h.min + width*float64(i),
		Max:   h.min + width*float64(i+1),
		Count: h.counts[i],
	}
}

type histogramOpts struct {
	approx         int
	includeOutside int
	min, max       float64
	buckets        int32
	errorHandler   ErrorHandler
}

// HistogramOption is an option that can be passed to Band.Histogram()
//
// Available HistogramOptions are:
//
// • Approximate() to allow the algorithm to operate on a subset of the full resolution data
//
// • Intervals(count int, min,max float64) to compute a histogram with count buckets, spanning [min,max].
//   Each bucket will be (max-min)/count wide. If not provided, the default histogram will be returned.
//
// • IncludeOutOfRange() to populate the first and last bucket with values under/over the specified min/max
//   when used in conjuntion with Intervals()
//
// • ErrLogger
type HistogramOption interface {
	setHistogramOpt(ho *histogramOpts)
}

type includeOutsideOpt struct{}

func (ioo includeOutsideOpt) setHistogramOpt(ho *histogramOpts) {
	ho.includeOutside = 1
}

// IncludeOutOfRange populates the first and last bucket with values under/over the specified min/max
// when used in conjuntion with Intervals()
func IncludeOutOfRange() interface {
	HistogramOption
} {
	return includeOutsideOpt{}
}

type approximateOkOption struct{}

func (aoo approximateOkOption) setHistogramOpt(ho *histogramOpts) {
	ho.approx = 1
}

// Approximate allows the histogram algorithm to operate on a subset of the full resolution data
func Approximate() interface {
	HistogramOption
} {
	return approximateOkOption{}
}

type intervalsOption struct {
	min, max float64
	buckets  int32
}

func (io intervalsOption) setHistogramOpt(ho *histogramOpts) {
	ho.min = io.min
	ho.max = io.max
	ho.buckets = io.buckets
}

// Intervals computes a histogram with count buckets, spanning [min,max].
// Each bucket will be (max-min)/count wide. If not provided, the default histogram will be returned.
func Intervals(count int, min, max float64) interface {
	HistogramOption
} {
	return intervalsOption{min: min, max: max, buckets: int32(count)}
}
