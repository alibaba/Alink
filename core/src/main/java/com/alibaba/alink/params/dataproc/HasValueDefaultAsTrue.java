package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasValueDefaultAsTrue<T> extends WithParams <T> {

	@NameCn("是否有权重值")
	@DescCn("是否有权重值")
	ParamInfo <Boolean> HAS_VALUE = ParamInfoFactory
		.createParamInfo("hasValue", Boolean.class)
		.setDescription("has weight or not")
		.setHasDefaultValue(true)
		.build();

	default Boolean getHasValue() {
		return getParams().get(HAS_VALUE);
	}

	default T setHasValue(Boolean value) {
		return set(HAS_VALUE, value);
	}
}
