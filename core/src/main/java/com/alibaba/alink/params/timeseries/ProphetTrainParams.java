package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.dl.HasPythonEnv;

public interface ProphetTrainParams<T> extends
	TimeSeriesTrainParams <T>, HasPythonEnv <T> {
}
