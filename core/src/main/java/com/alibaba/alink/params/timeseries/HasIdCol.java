package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasIdCol<T> extends WithParams <T> {

	ParamInfo <String> ID_COL = ParamInfoFactory
		.createParamInfo("idCol", String.class)
		.setAlias(new String[] {"idColName"})
		.setDescription("id col name")
		.setRequired()
		.build();

	default String getIdCol() {
		return get(ID_COL);
	}

	default T setIdCol(String value) {
		return set(ID_COL, value);
	}
}
