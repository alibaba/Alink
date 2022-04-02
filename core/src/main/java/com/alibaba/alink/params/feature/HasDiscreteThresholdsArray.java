package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasDiscreteThresholdsArray<T> extends WithParams <T> {
	@NameCn("离散个数阈值")
	@DescCn("离散个数阈值，每一列对应数组中一个元素。")
	ParamInfo <Integer[]> DISCRETE_THRESHOLDS_ARRAY = ParamInfoFactory
		.createParamInfo("discreteThresholdsArray", Integer[].class)
		.setDescription("discreteThreshold")
		.setHasDefaultValue(null)
		.build();

	default Integer[] getDiscreteThresholdsArray() {
		return get(DISCRETE_THRESHOLDS_ARRAY);
	}

	default T setDiscreteThresholdsArray(Integer... value) {
		return set(DISCRETE_THRESHOLDS_ARRAY, value);
	}

}
