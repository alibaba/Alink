package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasGroupColsDefaultAsNull<T> extends WithParams<T> {
	ParamInfo <String[]> GROUP_COLS = ParamInfoFactory
		.createParamInfo("groupCols", String[].class)
		.setDescription("group col names")
		.setAlias(new String[] {"groupColNames"})
		.setHasDefaultValue(null)
		.build();

	default String[] getGroupCols() {return get(GROUP_COLS);}

	default T setGroupCols(String... colNames) {return set(GROUP_COLS, colNames);}
}
