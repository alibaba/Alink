package com.alibaba.alink.params.tensorflow.savedmodel;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

/**
 * Specifies columns for interference with null default value.
 *
 * @see HasSelectedColsDefaultAsNull
 */
public interface HasInferSelectedColsDefaultAsNull<T> extends WithParams <T> {

	/**
	 * @cn-name 用于推理的列名数组
	 * @cn 用于推理的列名列表
	 */
	ParamInfo <String[]> INFER_SELECTED_COLS = ParamInfoFactory
		.createParamInfo("inferSelectedCols", String[].class)
		.setDescription("Column names used for inference")
		.setHasDefaultValue(null)
		.build();

	default String[] getInferSelectedCols() {
		return get(INFER_SELECTED_COLS);
	}

	default T setInferSelectedCols(String... colNames) {
		return set(INFER_SELECTED_COLS, colNames);
	}
}
