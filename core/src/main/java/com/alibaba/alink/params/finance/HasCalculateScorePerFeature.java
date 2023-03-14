package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasCalculateScorePerFeature<T> extends WithParams <T> {
	@NameCn("输出变量分")
	@DescCn("输出变量分")
	ParamInfo <Boolean> CALCULATE_SCORE_PER_FEATURE = ParamInfoFactory
		.createParamInfo("calculateScorePerFeature", Boolean.class)
		.setDescription("calculate score per feature")
		.setHasDefaultValue(false)
		.build();

	default Boolean getCalculateScorePerFeature() {
		return get(CALCULATE_SCORE_PER_FEATURE);
	}

	default T setCalculateScorePerFeature(Boolean value) {
		return set(CALCULATE_SCORE_PER_FEATURE, value);
	}
}
