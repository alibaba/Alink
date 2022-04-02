package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxLeaves<T> extends WithParams <T> {

	@NameCn("最大结点个数")
	@DescCn("最大结点个数")
	ParamInfo <Integer> MAX_LEAVES = ParamInfoFactory
		.createParamInfo("maxLeaves", Integer.class)
		.setDescription("Maximum number of nodes to be added.")
		.setHasDefaultValue(0)
		.build();

	default Integer getMaxLeaves() {
		return get(MAX_LEAVES);
	}

	default T setMaxLeaves(Integer maxLeaves) {
		return set(MAX_LEAVES, maxLeaves);
	}
}
