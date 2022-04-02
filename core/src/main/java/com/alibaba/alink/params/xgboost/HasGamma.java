package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasGamma<T> extends WithParams <T> {

	@NameCn("结点分裂最小损失变化")
	@DescCn("节点分裂最小损失变化")
	ParamInfo <Double> GAMMA = ParamInfoFactory
		.createParamInfo("gamma", Double.class)
		.setDescription("Minimum loss reduction required to make a further partition on a leaf node of the tree. "
			+ "The larger gamma is, the more conservative the algorithm will be.")
		.setHasDefaultValue(0.0)
		.build();

	default Double getGamma() {
		return get(GAMMA);
	}

	default T setGamma(Double gamma) {
		return set(GAMMA, gamma);
	}
}
