package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasInitRecommCol<T> extends WithParams <T> {
	ParamInfo <String> INIT_RECOMM_COL = ParamInfoFactory
		.createParamInfo("initRecommCol", String.class)
		.setAlias(new String[] {"initRecommColName"})
		.setDescription("initial recommendation column name")
		.setHasDefaultValue(null)
		.build();

	default String getInitRecommCol() {
		return get(INIT_RECOMM_COL);
	}

	default T setInitRecommCol(String value) {
		return set(INIT_RECOMM_COL, value);
	}
}
