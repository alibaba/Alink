package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasTreePartition<T> extends WithParams <T> {
	@NameCn("模型中树类型的边界")
	@DescCn("指定树类型的边界(1, 2)")
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