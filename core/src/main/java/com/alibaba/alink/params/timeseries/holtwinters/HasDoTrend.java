package com.alibaba.alink.params.timeseries.holtwinters;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasDoTrend<T> extends WithParams <T> {

	@NameCn("时间是否具有趋势性")
	@DescCn("时间是否具有趋势性")
	ParamInfo <Boolean> DO_TREND = ParamInfoFactory
		.createParamInfo("doTrend", Boolean.class)
		.setDescription("Whether time serial has trend or not.")
		.setHasDefaultValue(false)
		.build();

	default Boolean getDoTrend() {
		return get(DO_TREND);
	}

	default T setDoTrend(Boolean value) {
		return set(DO_TREND, value);
	}
}
