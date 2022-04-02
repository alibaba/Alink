package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxGarch<T> extends WithParams <T> {

	@NameCn("最大garch阶数")
	@DescCn("最大garch阶数")
	ParamInfo <Integer> MAX_GARCH = ParamInfoFactory
		.createParamInfo("maxGARCH", Integer.class)
		.setDescription("max garch")
		.setHasDefaultValue(5)
		.build();

	default Integer getMaxGARCH() {
		return get(MAX_GARCH);
	}

	default T setMaxGARCH(Integer value) {
		return set(MAX_GARCH, value);
	}
}
