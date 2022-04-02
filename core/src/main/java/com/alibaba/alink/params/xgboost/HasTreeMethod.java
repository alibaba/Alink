package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasTreeMethod<T> extends WithParams <T> {

	@NameCn("构建树的方法")
	@DescCn("构建树的方法")
	ParamInfo <TreeMethod> TREE_METHOD = ParamInfoFactory
		.createParamInfo("treeMethod", TreeMethod.class)
		.setDescription("The tree construction algorithm used in XGBoost.")
		.setHasDefaultValue(TreeMethod.AUTO)
		.build();

	default TreeMethod getTreeMethod() {
		return get(TREE_METHOD);
	}

	default T setTreeMethod(TreeMethod treeMethod) {
		return set(TREE_METHOD, treeMethod);
	}

	default T setTreeMethod(String treeMethod) {
		return set(TREE_METHOD, ParamUtil.searchEnum(TREE_METHOD, treeMethod));
	}

	enum TreeMethod {
		AUTO,
		EXACT,
		APPROX,
		HIST
	}
}
