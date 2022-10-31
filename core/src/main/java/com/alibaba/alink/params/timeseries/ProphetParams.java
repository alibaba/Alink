package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface ProphetParams<T> extends
	TimeSeriesPredictParams <T> {

	@NameCn("用来计算指标的采样数目")
	@DescCn("用来计算指标的采样数目，设置成0，不计算指标。")
	ParamInfo <Integer> UNCERTAINTY_SAMPLES = ParamInfoFactory
		.createParamInfo("uncertaintySamples", Integer.class)
		.setDescription("uncertainty_samples")
		.setHasDefaultValue(1000)
		.build();

	default Integer getUncertaintySamples() {
		return get(UNCERTAINTY_SAMPLES);
	}

	default T setUncertaintySamples(Integer value) {
		return set(UNCERTAINTY_SAMPLES, value);
	}

	@NameCn("节假日")
	@DescCn("节假日，格式是 playoff:2008-01-13,2009-01-03 superbowl: 2010-02-07,2014-02-02")
	ParamInfo <String> HOLIDAYS = ParamInfoFactory
		.createParamInfo("holidays", String.class)
		.setDescription("holidays")
		.setHasDefaultValue(null)
		.build();

	default String getHolidays() {
		return get(HOLIDAYS);
	}

	default T setHolidays(String value) {
		return set(HOLIDAYS, value);
	}



	@NameCn("holidays_prior_scale")
	@DescCn("holidays_prior_scale")
	ParamInfo <Double> HOLIDAYS_PRIOR_SCALE = ParamInfoFactory
		.createParamInfo("holidaysPriorScale", Double.class)
		.setDescription("holidays_prior_scale")
		.setHasDefaultValue(10.0)
		.build();

	default Double getHolidaysPriorScale() {
		return get(HOLIDAYS_PRIOR_SCALE);
	}

	default T setHolidaysPriorScale(Double growth) {
		return set(HOLIDAYS_PRIOR_SCALE, growth);
	}

	@NameCn("cap")
	@DescCn("cap")
	ParamInfo <Double> CAP = ParamInfoFactory
		.createParamInfo("cap", Double.class)
		.setDescription("cap")
		.setHasDefaultValue(null)
		.build();

	default Double getCap() {
		return get(CAP);
	}

	default T setCap(Double value) {
		return set(CAP, value);
	}

	@NameCn("floor")
	@DescCn("floor")
	ParamInfo <Double> FLOOR = ParamInfoFactory
		.createParamInfo("floor", Double.class)
		.setDescription("floor")
		.setHasDefaultValue(null)
		.build();

	default Double getFloor() {
		return get(FLOOR);
	}

	default T setFloor(Double value) {
		return set(FLOOR, value);
	}

	@NameCn("growth")
	@DescCn("growth")
	ParamInfo <Growth> GROWTH = ParamInfoFactory
		.createParamInfo("growth", Growth.class)
		.setDescription("growth")
		.setHasDefaultValue(Growth.LINEAR)
		.build();

	default Growth getGrowth() {
		return get(GROWTH);
	}

	default T setGrowth(Growth growth) {
		return set(GROWTH, growth);
	}

	default T setGrowth(String growth) {
		return set(GROWTH, ParamUtil.searchEnum(GROWTH, growth));
	}

	@NameCn("changepoints")
	@DescCn("changepoints")
	ParamInfo <String> CHANGE_POINTS = ParamInfoFactory
		.createParamInfo("changePoints", String.class)
		.setDescription("changepoints")
		.setHasDefaultValue(null)
		.build();

	default String getChangePoints() {
		return get(CHANGE_POINTS);
	}

	default T setChangePoints(String value) {
		return set(CHANGE_POINTS, value);
	}

	@NameCn("n_change_point")
	@DescCn("n_change_point")
	ParamInfo <Integer> N_CHANGE_POINT = ParamInfoFactory
		.createParamInfo("nChangePoint", Integer.class)
		.setDescription("n_change_point")
		.setHasDefaultValue(25)
		.build();

	default Integer getNChangePoint() {
		return get(N_CHANGE_POINT);
	}

	default T setNChangePoint(Integer growth) {
		return set(N_CHANGE_POINT, growth);
	}

	@NameCn("change_point_range")
	@DescCn("change_point_range")
	ParamInfo <Double> CHANGE_POINT_RANGE = ParamInfoFactory
		.createParamInfo("changePointRange", Double.class)
		.setDescription("change_point_range")
		.setHasDefaultValue(0.8)
		.build();

	default Double getChangePointRange() {
		return get(CHANGE_POINT_RANGE);
	}

	default T setChangePointRange(Double growth) {
		return set(CHANGE_POINT_RANGE, growth);
	}

	@NameCn("changepoint_prior_scale")
	@DescCn("changepoint_prior_scale")
	ParamInfo <Double> CHANGE_POINT_PRIOR_SCALE = ParamInfoFactory
		.createParamInfo("changePointPriorScale", Double.class)
		.setDescription("changepoint_prior_scale")
		.setHasDefaultValue(0.05)
		.build();

	default Double getChangePointPriorScale() {
		return get(CHANGE_POINT_PRIOR_SCALE);
	}

	default T setChangePointPriorScale(Double growth) {
		return set(CHANGE_POINT_PRIOR_SCALE, growth);
	}

	@NameCn("interval_width")
	@DescCn("interval_width")
	ParamInfo <Double> INTERVAL_WIDTH = ParamInfoFactory
		.createParamInfo("intervalWidth", Double.class)
		.setDescription("interval_width")
		.setHasDefaultValue(0.80)
		.build();

	default Double getIntervalWidth() {
		return get(INTERVAL_WIDTH);
	}

	default T setIntervalWidth(Double growth) {
		return set(INTERVAL_WIDTH, growth);
	}

	@NameCn("seasonality_mode")
	@DescCn("seasonality_mode")
	ParamInfo <SeasonalityMode> SEASONALITY_MODE = ParamInfoFactory
		.createParamInfo("seasonalityMode", SeasonalityMode.class)
		.setDescription("seasonality_mode")
		.setHasDefaultValue(SeasonalityMode.ADDITIVE)
		.build();

	default SeasonalityMode getSeasonalityMode() {
		return get(SEASONALITY_MODE);
	}

	default T setSeasonalityMode(SeasonalityMode growth) {
		return set(SEASONALITY_MODE, growth);
	}

	default T setSeasonalityMode(String growth) {
		return set(SEASONALITY_MODE, ParamUtil.searchEnum(SEASONALITY_MODE, growth));
	}

	@NameCn("seasonality_prior_scale")
	@DescCn("seasonality_prior_scale")
	ParamInfo <Double> SEASONALITY_PRIOR_SCALE = ParamInfoFactory
		.createParamInfo("seasonalityPriorScale", Double.class)
		.setDescription("seasonality_prior_scale")
		.setHasDefaultValue(10.0)
		.build();

	default Double getSeasonalityPriorScale() {
		return get(SEASONALITY_PRIOR_SCALE);
	}

	default T setSeasonalityPriorScale(Double growth) {
		return set(SEASONALITY_PRIOR_SCALE, growth);
	}

	@NameCn("yearly_seasonality")
	@DescCn("yearly_seasonality")
	ParamInfo <String> YEARLY_SEASONALITY = ParamInfoFactory
		.createParamInfo("yearlySeasonality", String.class)
		.setDescription("yearly_seasonality")
		.setHasDefaultValue("auto")
		.build();

	default String getYearlySeasonality() {
		return get(YEARLY_SEASONALITY);
	}

	default T setYearlySeasonality(String value) {
		return set(YEARLY_SEASONALITY, value);
	}

	@NameCn("weekly_seasonality")
	@DescCn("weekly_seasonality")
	ParamInfo <String> WEEKLY_SEASONALITY = ParamInfoFactory
		.createParamInfo("weeklySeasonality", String.class)
		.setDescription("weekly_seasonality")
		.setHasDefaultValue("auto")
		.build();

	default String getWeeklySeasonality() {
		return get(WEEKLY_SEASONALITY);
	}

	default T setWeeklySeasonality(String value) {
		return set(WEEKLY_SEASONALITY, value);
	}

	@NameCn("daily_seasonality")
	@DescCn("daily_seasonality")
	ParamInfo <String> DAILY_SEASONALITY = ParamInfoFactory
		.createParamInfo("dailySeasonality", String.class)
		.setDescription("daily_seasonality")
		.setHasDefaultValue("auto")
		.build();

	default String getDailySeasonality() {
		return get(DAILY_SEASONALITY);
	}

	default T setDailySeasonality(String value) {
		return set(DAILY_SEASONALITY, value);
	}


	@NameCn("mcmc_samples")
	@DescCn("mcmc_samples")
	ParamInfo <Integer> MCMC_SAMPLES = ParamInfoFactory
		.createParamInfo("mcmcSamples", Integer.class)
		.setDescription("mcmc_samples")
		.setHasDefaultValue(0)
		.build();

	default Integer getMcmcSamples() {
		return get(MCMC_SAMPLES);
	}

	default T setMcmcSamples(Integer growth) {
		return set(MCMC_SAMPLES, growth);
	}

	@NameCn("include_history")
	@DescCn("include_history")
	ParamInfo <Boolean> INCLUDE_HISTORY = ParamInfoFactory
		.createParamInfo("includeHistory", Boolean.class)
		.setDescription("include history")
		.setHasDefaultValue(Boolean.FALSE)
		.build();

	default Boolean getIncludeHistory() {
		return get(INCLUDE_HISTORY);
	}

	default T setIncludeHistory(Boolean growth) {
		return set(INCLUDE_HISTORY, growth);
	}

	@NameCn("初始值")
	@DescCn("初始值")
	ParamInfo <String> STAN_INIT = ParamInfoFactory
		.createParamInfo("stanInit", String.class)
		.setDescription("stan_init")
		.setHasDefaultValue(null)
		.build();

	default String getStanInit() {
		return get(STAN_INIT);
	}

	default T setStanInit(String value) {
		return set(STAN_INIT, value);
	}

	enum Growth {
		LINEAR,
		LOGISTIC,
		FLAT
	}

	enum SeasonalityMode {
		MULTIPLICATIVE,
		ADDITIVE
	}
}
