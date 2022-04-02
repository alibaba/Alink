package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Whether standardize training data or not, default is true.
 */
public interface HasStandardization<T> extends WithParams <T> {
	@NameCn("是否正则化")
	@DescCn("是否对训练数据做正则化，默认true")
	ParamInfo <Boolean> STANDARDIZATION = ParamInfoFactory
		.createParamInfo("standardization", Boolean.class)
		.setDescription("Whether standardize training data or not, default is true")
		.setHasDefaultValue(true)
		.build();

	default Boolean getStandardization() {
		return get(STANDARDIZATION);
	}

	default T setStandardization(Boolean value) {
		return set(STANDARDIZATION, value);
	}
}
