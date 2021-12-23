package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.dl.HasPythonEnv;

public interface ProphetPredictParams<T> extends
	TimeSeriesPredictParams <T>, HasPythonEnv <T> {

}
