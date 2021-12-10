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
	bForce       int
	errorHandler ErrorHandler
}

type StatisticsOption interface {
	setStatisticsOpt(so *statisticsOpts)
}

type approxOkOption struct{}

func (aoo approxOkOption) setStatisticsOpt(so *statisticsOpts) {
	so.approx = 1
}

//StatisticsAproximate allows the statistics to be computed on overviews or a subset of all tiles.
func StatisticsApproximate() interface {
	StatisticsOption
} {
	return approxOkOption{}
}

type bForceOption struct{}

func (boo bForceOption) setStatisticsOpt(so *statisticsOpts) {
	so.bForce = 0
}

//Force allows the pre-computed statistics to be return (no new statistics is computed).
func Force() interface {
	StatisticsOption
} {
	return bForceOption{}
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

//ClearStatistics  is an option passed to Dataset.ClearStatistics
//Available options are:
//  -ErrLogger

type ClearStatisticsOption interface {
	setClearStatisticsOpt(sts *clearStatisticsOpt)
}

type clearStatisticsOpt struct {
	errorHandler ErrorHandler
}
