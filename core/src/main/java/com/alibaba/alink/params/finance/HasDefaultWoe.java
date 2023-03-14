package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasDefaultWoe<T> extends WithParams <T> {

	@NameCn("默认Woe，在woe为Nan或NULL时替换")
	@DescCn("默认Woe，在woe为Nan或NULL时替换")
	ParamInfo <Double> DEFAULT_WOE = ParamInfoFactory
		.createParamInfo("defaultWoe", Double.class)
		.setDescription("default woe")
		.setHasDefaultValue(Double.NaN)
		.build();

	default Double getDefaultWoe() {
		return get(DEFAULT_WOE);
	}

	default T setDefaultWoe(Double value) {
		return set(DEFAULT_WOE, value);
	}
}
