package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Trait for parameter withMean. If true, center the data with mean before scaling.
 */
public interface HasWithMean<T> extends WithParams <T> {

	@NameCn("是否使用均值")
	@DescCn("是否使用均值，默认使用")
	ParamInfo <Boolean> WITH_MEAN = ParamInfoFactory
		.createParamInfo("withMean", Boolean.class)
		.setDescription("Centers the data with mean before scaling.")
		.setHasDefaultValue(true)
		.build();

	default Boolean getWithMean() {
		return get(WITH_MEAN);
	}

	default T setWithMean(Boolean value) {
		return set(WITH_MEAN, value);
	}
}
