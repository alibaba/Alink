package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.feature.featuregenerator.HasTimeCol;

public interface TimeSeriesTrainParams<T> extends
	HasValueCol <T>,
	HasTimeCol <T> {
}
