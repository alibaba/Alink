package com.alibaba.alink.operator.common.timeseries.holtwinter;

import com.alibaba.alink.params.timeseries.holtwinters.HasSeasonalType.SeasonalType;

public class HoltWintersModel {
	double[] coefs;

	Double levelStart;
	Double trendStart;
	double[] seasonalStart;

	SeasonalType seasonalType;

	int frequency;

	public double[] forecast(int predictNum) {
		boolean isAddType = seasonalType == SeasonalType.ADDITIVE;
		return HoltWintersUtil.holtWintersForecast(predictNum,
			coefs, isAddType, frequency,
			levelStart, trendStart, seasonalStart).getData();
	}

	//a, b, s are the initialization data of level, trend and seasonalPeriod.

}
