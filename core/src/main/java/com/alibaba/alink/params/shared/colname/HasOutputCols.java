package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * An interface for classes with a parameter specifying names of multiple output columns.
 *
 * @see HasOutputCol
 * @see HasOutputColDefaultAsNull
 * @see HasOutputColsDefaultAsNull
 */
public interface HasOutputCols<T> extends WithParams <T> {

	@NameCn("输出结果列列名数组")
	@DescCn("输出结果列列名数组，必选")
	ParamInfo <String[]> OUTPUT_COLS = ParamInfoFactory
		.createParamInfo("outputCols", String[].class)
		.setDescription("Names of the output columns")
		.setAlias(new String[] {"outputColNames"})
		.setRequired()
		.build();

	default String[] getOutputCols() {
		return get(OUTPUT_COLS);
	}

	default T setOutputCols(String... colNames) {
		return set(OUTPUT_COLS, colNames);
	}
}
