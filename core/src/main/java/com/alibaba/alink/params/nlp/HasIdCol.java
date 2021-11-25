package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasIdCol<T> extends WithParams <T> {
	/**
	 * @cn-name id列名
	 * @cn id列名
	 */
	ParamInfo <String> ID_COL = ParamInfoFactory
		.createParamInfo("idCol", String.class)
		.setDescription("id colname")
		.setRequired()
		.setAlias(new String[] {"idColName"})
		.build();

	default String getIdCol() {
		return get(ID_COL);
	}

	default T setIdCol(String value) {
		return set(ID_COL, value);
	}

}
