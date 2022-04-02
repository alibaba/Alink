package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasRefreshLeaf<T> extends WithParams <T> {

	@NameCn("RefreshLeaf")
	@DescCn("RefreshLeaf")
	ParamInfo <Integer> REFRESH_LEAF = ParamInfoFactory
		.createParamInfo("refreshLeaf", Integer.class)
		.setDescription("This is a parameter of the refresh updater.")
		.setHasDefaultValue(1)
		.build();

	default Integer getRefreshLeaf() {
		return get(REFRESH_LEAF);
	}

	default T setRefreshLeaf(Integer maxDepth) {
		return set(REFRESH_LEAF, maxDepth);
	}
}
