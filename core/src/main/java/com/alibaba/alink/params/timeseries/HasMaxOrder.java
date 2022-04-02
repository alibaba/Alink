package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxOrder<T> extends WithParams <T> {

	@NameCn("模型(p, q)上限")
	@DescCn("模型(p, q)上限")
	ParamInfo <Integer> MAX_ORDER = ParamInfoFactory
		.createParamInfo("maxOrder", Integer.class)
		.setDescription("max order of p, q")
		.setHasDefaultValue(10)
		.setAlias(new String[] {"upperBound"})
		.build();

	default Integer getMaxOrder() {
		return get(MAX_ORDER);
	}

	default T setMaxOrder(Integer value) {
		return set(MAX_ORDER, value);
	}
}
