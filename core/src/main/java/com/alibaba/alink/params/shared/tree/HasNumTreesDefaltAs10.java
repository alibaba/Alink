package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.validators.MinValidator;

public interface HasNumTreesDefaltAs10<T> extends WithParams <T> {
	@NameCn("模型中树的棵数")
	@DescCn("模型中树的棵数")
	ParamInfo <Integer> NUM_TREES = ParamInfoFactory
		.createParamInfo("numTrees", Integer.class)
		.setDescription("Number of decision trees.")
		.setHasDefaultValue(10)
		.setValidator(new MinValidator <>(1))
		.setAlias(new String[] {"treeNum"})
		.build();

	default Integer getNumTrees() {
		return get(NUM_TREES);
	}

	default T setNumTrees(Integer value) {
		return set(NUM_TREES, value);
	}
}
