package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Param: columns whose type are string or boolean.
 */
public interface HasCategoricalCols<T> extends WithParams <T> {
	@NameCn("离散特征列名")
	@DescCn("离散特征列名")
	ParamInfo <String[]> CATEGORICAL_COLS = ParamInfoFactory
		.createParamInfo("categoricalCols", String[].class)
		.setDescription("Names of the categorical columns used for training in the input table")
		.setAlias(new String[] {"categoricalColNames"})
		.build();

	default String[] getCategoricalCols() {
		return get(CATEGORICAL_COLS);
	}

	default T setCategoricalCols(String... colNames) {
		return set(CATEGORICAL_COLS, colNames);
	}
}
