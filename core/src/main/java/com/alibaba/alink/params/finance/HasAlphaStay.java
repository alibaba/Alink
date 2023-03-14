package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasAlphaStay<T> extends WithParams <T> {

	@NameCn("移除阈值")
	@DescCn("移除阈值")
	ParamInfo <Double> ALPHA_STAY = ParamInfoFactory
		.createParamInfo("alphaStay", Double.class)
		.setDescription("select threshold backward")
		.setOptional()
		.setHasDefaultValue(0.05)
		.build();

	default Double getAlphaStay() {
		return get(ALPHA_STAY);
	}

	default T setAlphaStay(Double value) {
		return set(ALPHA_STAY, value);
	}
}
