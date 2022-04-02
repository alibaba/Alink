package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

/**
 * Trait for parameter strategy. It is the fill missing value strategy, support meam, max, min or value.
 */
public interface HasStrategy<T> extends WithParams <T> {

	@NameCn("缺失值填充规则")
	@DescCn("缺失值填充的规则，支持mean，max，min或者value。选择value时，需要读取fillValue的值")
	ParamInfo <Strategy> STRATEGY = ParamInfoFactory
		.createParamInfo("strategy", Strategy.class)
		.setDescription("the startegy to fill missing value, support mean, max, min or value")
		.setHasDefaultValue(Strategy.MEAN)
		.build();

	default Strategy getStrategy() {
		return get(STRATEGY);
	}

	default T setStrategy(Strategy value) {
		return set(STRATEGY, value);
	}

	default T setStrategy(String value) {
		return set(STRATEGY, ParamUtil.searchEnum(STRATEGY, value));
	}

	enum Strategy {
		MEAN,
		MIN,
		MAX,
		VALUE
	}
}
