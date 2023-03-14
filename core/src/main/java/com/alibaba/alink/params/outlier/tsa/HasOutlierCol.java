package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasOutlierCol<T> extends WithParams <T> {

	/**
	 * @name 异常指示列
	 */
	ParamInfo <String> OUTLIER_COL = ParamInfoFactory
		.createParamInfo("outlierCol", String.class)
		.setDescription("hasOutlierCol")
		.setRequired()
		.build();

	default String getOutlierCol() {
		return get(OUTLIER_COL);
	}

	default T setOutlierCol(String value) {
		return set(OUTLIER_COL, value);
	}
}
