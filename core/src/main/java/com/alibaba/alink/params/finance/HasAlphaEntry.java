package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasAlphaEntry<T> extends WithParams <T> {
	@NameCn("筛选阈值")
	@DescCn("筛选阈值")
	ParamInfo <Double> ALPHA_ENTRY = ParamInfoFactory
		.createParamInfo("alphaEntry", Double.class)
		.setDescription("select threshold forward")
		.setOptional()
		.setHasDefaultValue(0.05)
		.build();

	default Double getAlphaEntry() {
		return get(ALPHA_ENTRY);
	}

	default T setAlphaEntry(Double value) {
		return set(ALPHA_ENTRY, value);
	}

}
