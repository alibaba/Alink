package com.alibaba.alink.params.outlier.tsa.TsaAlgoParams;

import com.alibaba.alink.params.timeseries.HasFrequency;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalType;

public interface ConvolutionDecomposeAlgoParams<T> extends
	HasFrequency <T>,
	HasSeasonalType <T> {
}
