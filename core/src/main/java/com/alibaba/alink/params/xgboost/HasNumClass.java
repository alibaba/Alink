package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasNumClass<T> extends WithParams <T> {

	@NameCn("标签类别个数")
	@DescCn("标签类别个数， 多分类时有效")
	ParamInfo <Integer> NUM_CLASS = ParamInfoFactory
		.createParamInfo("numClass", Integer.class)
		.setDescription("number of classes.")
		.setHasDefaultValue(0)
		.build();

	default Integer getNumClass() {
		return get(NUM_CLASS);
	}

	default T setNumClass(Integer numClass) {
		return set(NUM_CLASS, numClass);
	}
}
