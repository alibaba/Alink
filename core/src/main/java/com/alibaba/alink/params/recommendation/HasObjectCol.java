package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasObjectCol<T> extends WithParams <T> {
	/**
	 * @cn-name Object列列名
	 * @cn Object列列名
	 */
	ParamInfo <String> OBJECT_COL = ParamInfoFactory
		.createParamInfo("objectCol", String.class)
		.setAlias(new String[] {"objectColName"})
		.setDescription("Object column name")
		.setRequired()
		.build();

	default String getObjectCol() {
		return get(OBJECT_COL);
	}

	default T setObjectCol(String value) {
		return set(OBJECT_COL, value);
	}
}
