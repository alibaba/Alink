package com.alibaba.alink.params.dataproc.vector;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasVectorImputerFillValue<T> extends WithParams <T> {
	/**
	 * @cn-name 填充缺失值
	 * @cn 自定义的填充值。当strategy为value时，读取fillValue的值
	 */
	ParamInfo <Double> FILL_VALUE = ParamInfoFactory
		.createParamInfo("fillValue", Double.class)
		.setDescription("fill all missing values with fillValue")
		.setHasDefaultValue(null)
		.build();

	default Double getFillValue() {
		return get(FILL_VALUE);
	}

	default T setFillValue(Double value) {
		return set(FILL_VALUE, value);
	}
}
