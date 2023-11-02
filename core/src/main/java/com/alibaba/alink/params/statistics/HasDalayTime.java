package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasDalayTime<T> extends WithParams <T> {
	@NameCn("延迟时间")
	@DescCn("延迟时间")
	ParamInfo <Integer> DALAY_TIME = ParamInfoFactory
		.createParamInfo("dalayTime", Integer.class)
		.setDescription("delay time")
		.setHasDefaultValue(0)
		.build();

	default Integer getDalayTime() {
		return get(DALAY_TIME);
	}

	default T setDalayTime(Integer value) {
		return set(DALAY_TIME, value);
	}
}
