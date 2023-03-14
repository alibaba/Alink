package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasConstraint<T> extends WithParams <T> {

	@NameCn("约束条件")
	@DescCn("约束条件")
	ParamInfo <String> CONSTRAINT = ParamInfoFactory
		.createParamInfo("constraint", String.class)
		.setDescription("constraint")
		.setHasDefaultValue(null)
		.build();

	default String getConstraint() {
		return get(CONSTRAINT);
	}

	default T setConstraint(String value) {
		return set(CONSTRAINT, value);
	}
}
