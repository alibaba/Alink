package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.timeseries.holtwinters.HasAlpha;
import com.alibaba.alink.params.timeseries.holtwinters.HasBeta;
import com.alibaba.alink.params.timeseries.holtwinters.HasDoSeasonal;
import com.alibaba.alink.params.timeseries.holtwinters.HasDoTrend;
import com.alibaba.alink.params.timeseries.holtwinters.HasGamma;
import com.alibaba.alink.params.timeseries.holtwinters.HasLevelStart;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalStart;
import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalType;
import com.alibaba.alink.params.timeseries.holtwinters.HasTrendStart;

public interface HoltWintersAlgoParams<T> extends
	HasAlpha <T>,
	HasBeta <T>,
	HasGamma <T>,
	HasFrequency <T>,
	HasDoTrend <T>,
	HasDoSeasonal <T>,
	HasSeasonalType <T>,
	HasLevelStart <T>,
	HasTrendStart <T>,
	HasSeasonalStart <T> {

}
