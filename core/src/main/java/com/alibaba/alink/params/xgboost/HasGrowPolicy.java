package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasGrowPolicy<T> extends WithParams <T> {

	@NameCn("GrowPolicy")
	@DescCn("GrowPolicy")
	ParamInfo <GrowPolicy> GROW_POLICY = ParamInfoFactory
		.createParamInfo("growPolicy", GrowPolicy.class)
		.setDescription("A type of boosting process to run.")
		.setHasDefaultValue(GrowPolicy.DEPTH_WISE)
		.build();

	default GrowPolicy getGrowPolicy() {
		return get(GROW_POLICY);
	}

	default T setGrowPolicy(GrowPolicy growPolicy) {
		return set(GROW_POLICY, growPolicy);
	}

	default T setGrowPolicy(String growPolicy) {
		return set(GROW_POLICY, ParamUtil.searchEnum(GROW_POLICY, growPolicy));
	}

	enum GrowPolicy {
		DEPTH_WISE,
		LOSS_GUIDE
	}
}
