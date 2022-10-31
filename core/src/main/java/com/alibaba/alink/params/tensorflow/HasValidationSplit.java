package com.alibaba.alink.params.tensorflow;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasValidationSplit<T> extends WithParams <T> {
	@NameCn("验证集比例")
	@DescCn("验证集比例，仅在总并发度为 1 时生效")
	ParamInfo <Double> VALIDATION_SPLIT = ParamInfoFactory
		.createParamInfo("validationSplit", Double.class)
		.setDescription("Split ratio for validation set, only works when total parallelism is 1")
		.setHasDefaultValue(0.)
		.build();

	default Double getValidationSplit() {
		return get(VALIDATION_SPLIT);
	}

	default T setValidationSplit(Double value) {
		return set(VALIDATION_SPLIT, value);
	}
}
