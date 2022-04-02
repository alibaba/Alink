package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface LeaveTopKObjectOutParams<T> extends
	LeaveKObjectOutParams <T>,
	HasRateCol <T> {

	@NameCn("打分阈值")
	@DescCn("打分阈值")
	ParamInfo <Double> RATE_THRESHOLD = ParamInfoFactory
		.createParamInfo("rateThreshold", Double.class)
		.setDescription("rate threshold")
		.setHasDefaultValue(Double.NEGATIVE_INFINITY)
		.setAlias(new String[] {"threshold"})
		.build();

	default Double getRateThreshold() {
		return get(RATE_THRESHOLD);
	}

	default T setRateThreshold(Double value) {
		return set(RATE_THRESHOLD, value);
	}
}
