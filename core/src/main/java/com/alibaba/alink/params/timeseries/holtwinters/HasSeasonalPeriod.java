package com.alibaba.alink.params.timeseries.holtwinters;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.validators.MinValidator;

public interface HasSeasonalPeriod<T> extends WithParams <T> {


	ParamInfo <Integer> SEASONAL_PERIOD = ParamInfoFactory
		.createParamInfo("seasonalPeriod", Integer.class)
		.setDescription("The seasonalPeriod period.")
		.setHasDefaultValue(1)
		.setValidator(new MinValidator <>(1))
		.build();

	default Integer getSeasonalPeriod() {
		return get(SEASONAL_PERIOD);
	}

	default T setSeasonalPeriod(Integer value) {
		return set(SEASONAL_PERIOD, value);
	}
}
