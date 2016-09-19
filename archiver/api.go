package archiver

import (
	"github.com/gtfierro/durandal/common"
)

func (a *Archiver) SelectTags(vk string, params *common.TagParams) ([]common.MetadataGroup, error) {
	return a.MD.GetMetadata(vk, params.Tags, params.Where)
}

func (a *Archiver) DistinctTag(vk string, params *common.DistinctParams) ([]string, error) {
	return a.MD.GetDistinct(vk, params.Tag, params.Where)
}

// selects data for the matching streams within the range given
// by Begin/End
func (a *Archiver) SelectDataRange(params *common.DataParams) ([]common.Timeseries, error) {
	var (
		err    error
		result []common.Timeseries
	)
	if err = a.prepareDataParams(params); err != nil {
		return result, err
	}

	// switch order so its consistent
	if params.End < params.Begin {
		params.Begin, params.End = params.End, params.Begin
	}

	// fetch readings
	result, err = a.TS.GetData(params.UUIDs, params.Begin, params.End)
	if err != nil {
		return result, err
	}

	// convert readings into the correct unit of time
	result = a.packResults(params, result)

	return result, nil
}

// selects the data point most immediately before the Start parameter for all matching streams
func (a *Archiver) SelectDataBefore(params *common.DataParams) (result []common.Timeseries, err error) {
	if err = a.prepareDataParams(params); err != nil {
		return
	}
	result, err = a.TS.Prev(params.UUIDs, params.Begin)
	result = a.packResults(params, result)
	return
}

// selects the data point most immediately after the Start parameter for all matching streams
func (a *Archiver) SelectDataAfter(params *common.DataParams) (result []common.Timeseries, err error) {
	if err = a.prepareDataParams(params); err != nil {
		return
	}
	result, err = a.TS.Next(params.UUIDs, params.Begin)
	result = a.packResults(params, result)
	return
}

func (a *Archiver) SelectStatisticalData(params *common.DataParams) (result []common.StatisticTimeseries, err error) {
	if err = a.prepareDataParams(params); err != nil {
		return
	}
	// switch order so its consistent
	if params.End < params.Begin {
		params.Begin, params.End = params.End, params.Begin
	}
	if params.IsStatistical {
		result, err = a.TS.StatisticalData(params.UUIDs, params.PointWidth, params.Begin, params.End)
	} else if params.IsWindow {
		result, err = a.TS.WindowData(params.UUIDs, params.Width, params.Begin, params.End)
	}
	result = a.packStatsResults(params, result)
	return
}

//
//func (a *Archiver) DeleteData(params *common.DataParams) (err error) {
//	if err = a.prepareDataParams(params); err != nil {
//		return
//	}
//	// switch order so its consistent
//	if params.End < params.Begin {
//		params.Begin, params.End = params.End, params.Begin
//	}
//	return a.TS.DeleteData(params.UUIDs, params.Begin, params.End)
//}
//
func (a *Archiver) prepareDataParams(params *common.DataParams) (err error) {
	// parse and evaluate the where clause if we need to
	if len(params.Where) > 0 {
		params.UUIDs, err = a.MD.GetUUIDs("", params.Where)
		if err != nil {
			return err
		}
	}

	// apply the streamlimit if it exists
	if params.StreamLimit > 0 && len(params.UUIDs) > params.StreamLimit {
		params.UUIDs = params.UUIDs[:params.StreamLimit]
	}

	// make sure that Begin/End are both in nanoseconds
	if begin_uot := common.GuessTimeUnit(params.Begin); begin_uot != common.UOT_NS {
		params.Begin, err = common.ConvertTime(params.Begin, begin_uot, common.UOT_NS)
		if err != nil {
			return err
		}
	}
	if end_uot := common.GuessTimeUnit(params.End); end_uot != common.UOT_NS {
		params.End, err = common.ConvertTime(params.End, end_uot, common.UOT_NS)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *Archiver) packResults(params *common.DataParams, readings []common.Timeseries) []common.Timeseries {
	for i, resp := range readings {
		resp.Lock()
		if len(resp.Records) > 0 {
			// apply data limit if exists
			if params.DataLimit > 0 && len(resp.Records) > params.DataLimit {
				resp.Records = resp.Records[:params.DataLimit]
			}
			readings[i] = resp
		}
		resp.Unlock()
	}
	log.Debugf("Returning %d readings", len(readings))
	return readings
}

func (a *Archiver) packStatsResults(params *common.DataParams, readings []common.StatisticTimeseries) []common.StatisticTimeseries {
	for i, resp := range readings {
		resp.Lock()
		if len(resp.Records) > 0 {
			// apply data limit if exists
			if params.DataLimit > 0 && len(resp.Records) > params.DataLimit {
				resp.Records = resp.Records[:params.DataLimit]
			}
			readings[i] = resp
		}
	}
	log.Debugf("Returning %d readings", len(readings))
	return readings
}
