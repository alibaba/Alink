package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * num class of multi class train.
 */
public interface HasNumClass<T> extends WithParams <T> {
	@NameCn("类别数")
	@DescCn("多分类的类别数，必选")
	ParamInfo <Integer> NUM_CLASS = ParamInfoFactory
		.createParamInfo("numClass", Integer.class)
		.setDescription("num class of multi class train.")
		.setRequired()
		.setAlias(new String[] {"nClass"})
		.build();

	default Integer getNumClass() {
		return get(NUM_CLASS);
	}

	default T setNumClass(Integer value) {
		return set(NUM_CLASS, value);
	}
}
