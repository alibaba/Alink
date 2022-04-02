package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMinusMean<T> extends WithParams <T> {

	@NameCn("是否减去均值")
	@DescCn("是否减去均值")
	ParamInfo <Boolean> MINUS_MEAN = ParamInfoFactory
		.createParamInfo("minusMean", Boolean.class)
		.setDescription("minusMean")
		.setHasDefaultValue(true)
		.build();

	default Boolean getMinusMean() {
		return get(MINUS_MEAN);
	}

	default T setMinusMean(Boolean value) {
		return set(MINUS_MEAN, value);
	}
}
