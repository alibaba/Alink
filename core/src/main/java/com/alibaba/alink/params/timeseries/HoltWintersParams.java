package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.timeseries.HoltWintersAlgoParams;
import com.alibaba.alink.params.timeseries.TimeSeriesPredictParams;

public interface HoltWintersParams<T> extends
	TimeSeriesPredictParams<T>,
	HoltWintersAlgoParams <T> {}
