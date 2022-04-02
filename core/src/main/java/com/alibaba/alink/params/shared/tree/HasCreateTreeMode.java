package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasCreateTreeMode<T> extends WithParams <T> {
	@NameCn("创建树的模式。")
	@DescCn("series表示每个单机创建单颗树，parallel表示并行创建单颗树。")
	ParamInfo <String> CREATE_TREE_MODE = ParamInfoFactory
		.createParamInfo("createTreeMode", String.class)
		.setDescription("series or parallel")
		.setHasDefaultValue("series")
		.build();

	default String getCreateTreeMode() {
		return get(CREATE_TREE_MODE);
	}

	default T setCreateTreeMode(String value) {
		return set(CREATE_TREE_MODE, value);
	}
}
