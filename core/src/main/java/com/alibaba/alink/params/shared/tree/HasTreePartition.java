package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTreePartition<T> extends WithParams <T> {
	/**
	 * @cn-name 模型中树类型的边界
	 * @cn 指定树类型的边界(1, 2)
	 */
	ParamInfo <String> TREE_PARTITION = ParamInfoFactory
		.createParamInfo("treePartition", String.class)
		.setDescription("The partition of the tree.")
		.setHasDefaultValue("0,0")
		.build();

	default String getTreePartition() {
		return get(TREE_PARTITION);
	}

	default T setTreePartition(String value) {
		return set(TREE_PARTITION, value);
	}
}