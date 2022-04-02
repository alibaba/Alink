package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasLifeCycleDefaultAsNeg1<T> extends WithParams <T> {

	@NameCn("生命周期")
	@DescCn("生命周期，默认-1")
	ParamInfo <Long> LIFE_CYCLE = ParamInfoFactory
		.createParamInfo("lifeCycle", Long.class)
		.setDescription("life cycle")
		.setHasDefaultValue(-1L)
		.setAlias(new String[] {"lifecycle"})
		.build();

	default Long getLifeCycle() {
		return get(LIFE_CYCLE);
	}

	default T setLifeCycle(Long value) {
		return set(LIFE_CYCLE, value);
	}
}
