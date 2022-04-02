package com.alibaba.alink.params.timeseries.holtwinters;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasLevelStart<T> extends WithParams <T> {

	@NameCn("level初始值")
	@DescCn("level初始值")
	ParamInfo <Double> LEVEL_START = ParamInfoFactory
		.createParamInfo("levelStart", Double.class)
		.setDescription("The level start.")
		.build();

	default Double getLevelStart() {
		return get(LEVEL_START);
	}

	default T setLevelStart(Double value) {
		return set(LEVEL_START, value);
	}

}
