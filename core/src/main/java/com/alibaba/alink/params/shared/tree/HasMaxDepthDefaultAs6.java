package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxDepthDefaultAs6<T> extends WithParams <T> {
	@NameCn("树的深度限制")
	@DescCn("树的深度限制")
	ParamInfo <Integer> MAX_DEPTH = ParamInfoFactory
		.createParamInfo("maxDepth", Integer.class)
		.setDescription("depth of the tree")
		.setHasDefaultValue(6)
		.setAlias(new String[] {"depth"})
		.build();

	default Integer getMaxDepth() {
		return get(MAX_DEPTH);
	}

	default T setMaxDepth(Integer value) {
		return set(MAX_DEPTH, value);
	}
}
