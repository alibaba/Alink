package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxSeasonalOrder<T> extends WithParams <T> {

	@NameCn("季节模型(p, q)上限")
	@DescCn("季节模型(p, q)上限")
	ParamInfo <Integer> MAX_SEASONAL_ORDER = ParamInfoFactory
		.createParamInfo("maxSeasonalOrder", Integer.class)
		.setDescription("seasonality upper bound")
		.setHasDefaultValue(1)
		.setAlias(new String[] {"seasonality"})
		.build();

	default Integer getMaxSeasonalOrder() {
		return get(MAX_SEASONAL_ORDER);
	}

	default T setMaxSeasonalOrder(Integer value) {
		return set(MAX_SEASONAL_ORDER, value);
	}
}
