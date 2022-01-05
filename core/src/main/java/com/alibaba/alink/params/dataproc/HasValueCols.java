package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasValueCols<T> extends WithParams <T> {
	/**
	 * @cn 多数值列
	 * @cn-name 多数值列
	 */
	ParamInfo <String[]> VALUE_COLS = ParamInfoFactory
		.createParamInfo("valueCols", String[].class)
		.setDescription("value colume names")
		.setHasDefaultValue(null)
		.build();

	default String[] getValueCols() {return get(VALUE_COLS);}

	default T setValueCols(String... value) {return set(VALUE_COLS, value);}
}
