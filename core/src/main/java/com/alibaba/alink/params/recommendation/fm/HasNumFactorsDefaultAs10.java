package com.alibaba.alink.params.recommendation.fm;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * num factor.
 */
public interface HasNumFactorsDefaultAs10<T> extends WithParams <T> {
	@NameCn("因子数")
	@DescCn("因子数")
	ParamInfo <Integer> NUM_FACTOR = ParamInfoFactory
		.createParamInfo("numFactor", Integer.class)
		.setDescription("number of factor")
		.setHasDefaultValue(10)
		.build();

	default Integer getNumFactor() {
		return get(NUM_FACTOR);
	}

	default T setNumFactor(Integer value) {
		return set(NUM_FACTOR, value);
	}

}
