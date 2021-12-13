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

// Statisitics on a given band.
type Statistics struct {
	Min, Max, Mean, Std float64
}

type statisticsOpts struct {
	approx       int
	errorHandler ErrorHandler
}

//StatisticsOption is an option that can be passed to Band.Statistics
//
//Available Statistics options are:
// - Aproximate() to allow the satistics to be computed on overviews or a subset od all tiles.
// - ErrLogger
type StatisticsOption interface {
	setStatisticsOpt(so *statisticsOpts)
}

func (aoo approximateOkOption) setStatisticsOpt(so *statisticsOpts) {
	so.approx = 1
}

//SetStatistics is an option that can passed to Band.SetStatistics()
//Available options are:
//  -ErrLogger
type SetStatisticsOption interface {
	setSetStatisticsOpt(sts *setStatisticsOpt)
}

type setStatisticsOpt struct {
	errorHandler ErrorHandler
}

//GetStatistics is an option that can passed to band.GetStatistics()
//Available options are:
//   -ErrLogger
type GetStatisticsOption interface {
	setGetStatisticsOpt(gts *getStatisticsOpt)
}

type getStatisticsOpt struct {
	errorHandler ErrorHandler
}

//ClearStatistics  is an option passed to Dataset.ClearStatistics
//Available options are:
//  -ErrLogger

type ClearStatisticsOption interface {
	setClearStatisticsOpt(sts *clearStatisticsOpt)
}

type clearStatisticsOpt struct {
	errorHandler ErrorHandler
}
