package com.alibaba.alink.params.timeseries.holtwinters;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasSeasonalType<T> extends WithParams <T> {

	/**
	 * @cn-name 季节类型
	 * @cn 季节类型
	 */
	ParamInfo <SeasonalType> SEASONAL_TYPE = ParamInfoFactory
		.createParamInfo("seasonalType", SeasonalType.class)
		.setDescription("Use additive or multiplicative. True is additive.")
		.setHasDefaultValue(SeasonalType.ADDITIVE)
		.build();

	default SeasonalType getSeasonalType() {
		return get(SEASONAL_TYPE);
	}

	default T setSeasonalType(SeasonalType value) {
		return set(SEASONAL_TYPE, value);
	}

	default T setSeasonalType(String value) {
		return set(SEASONAL_TYPE, ParamUtil.searchEnum(SEASONAL_TYPE, value));
	}

	enum SeasonalType {
		MULTIPLICATIVE,
		ADDITIVE
	}
}
