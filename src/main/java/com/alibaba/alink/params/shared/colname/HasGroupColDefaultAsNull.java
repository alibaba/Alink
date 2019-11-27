package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasGroupColDefaultAsNull<T> extends WithParams<T> {
	ParamInfo <String> GROUP_COL = ParamInfoFactory
		.createParamInfo("groupCol", String.class)
		.setDescription("Name of a grouping column")
		.setAlias(new String[] {"groupColName"})
		.setHasDefaultValue(null)
		.build();

	default String getGroupCol() {
		return get(GROUP_COL);
	}

	default T setGroupCol(String colName) {
		return set(GROUP_COL, colName);
	}
}
