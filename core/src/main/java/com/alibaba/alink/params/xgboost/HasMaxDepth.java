package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxDepth<T> extends WithParams <T> {

	@NameCn("最大深度")
	@DescCn("最大深度")
	ParamInfo <Integer> MAX_DEPTH = ParamInfoFactory
		.createParamInfo("maxDepth", Integer.class)
		.setDescription("Maximum depth of a tree. "
			+ "Increasing this value will make the model more complex and more likely to overfit.")
		.setHasDefaultValue(6)
		.build();

	default Integer getMaxDepth() {
		return get(MAX_DEPTH);
	}

	default T setMaxDepth(Integer maxDepth) {
		return set(MAX_DEPTH, maxDepth);
	}
}
