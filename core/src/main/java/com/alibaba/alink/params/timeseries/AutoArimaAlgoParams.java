package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalPeriod;

public interface AutoArimaAlgoParams<T> extends
	HasEstmateMethod <T>,
	HasIcType <T>,
	HasMaxOrder <T>,
	HasMaxSeasonalOrder <T>,
	HasSeasonalPeriod<T> {
}
