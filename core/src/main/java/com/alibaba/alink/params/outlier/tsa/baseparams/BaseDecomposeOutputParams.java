package com.alibaba.alink.params.outlier.tsa.baseparams;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface BaseDecomposeOutputParams<T> extends WithParams <T> {

	/**
	 * @name 原时间序列的时序构成列
	 */
	ParamInfo <String> TIME_COL = ParamInfoFactory
		.createParamInfo("timeCol", String.class)
		.setDescription("time column")
		.setRequired()
		.build();

	default String getTimeCol() {
		return get(TIME_COL);
	}

	default T setTimeCol(String value) {
		return set(TIME_COL, value);
	}

	/**
	 * @name 趋势列
	 */
	ParamInfo <String> TREND_COL = ParamInfoFactory
		.createParamInfo("trendCol", String.class)
		.setDescription("trend column")
		.setRequired()
		.build();

	default String getTrendCol() {
		return get(TREND_COL);
	}

	default T setTrendCol(String value) {
		return set(TREND_COL, value);
	}

	/**
	 * @name 季节列
	 */
	ParamInfo <String> SEASONAL_COL = ParamInfoFactory
		.createParamInfo("seasonalCol", String.class)
		.setDescription("seasonal column")
		.setRequired()
		.build();

	default String getSeasonalCol() {
		return get(SEASONAL_COL);
	}

	default T setSeasonalCol(String value) {
		return set(SEASONAL_COL, value);
	}

	/**
	 * @name 残差列
	 */
	ParamInfo <String> REMINDER_COL = ParamInfoFactory
		.createParamInfo("reminderCol", String.class)
		.setDescription("reminder column")
		.setRequired()
		.build();

	default String getReminderCol() {
		return get(REMINDER_COL);
	}

	default T setReminderCol(String value) {
		return set(REMINDER_COL, value);
	}

}
