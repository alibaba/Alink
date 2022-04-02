package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxBin<T> extends WithParams <T> {

	@NameCn("最大结点个数")
	@DescCn("最大结点个数")
	ParamInfo <Integer> MAX_BIN = ParamInfoFactory
		.createParamInfo("maxBin", Integer.class)
		.setDescription("Maximum number of discrete bins to bucket continuous features.")
		.setHasDefaultValue(256)
		.build();

	default Integer getMaxBin() {
		return get(MAX_BIN);
	}

	default T setMaxBin(Integer maxLeaves) {
		return set(MAX_BIN, maxLeaves);
	}
}
