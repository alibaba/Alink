package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * An interface for classes with a parameter specifying names of multiple output columns. The default parameter value is
 * null.
 *
 * @see HasOutputCol
 * @see HasOutputColDefaultAsNull
 * @see HasOutputCols
 */
public interface HasOutputColsDefaultAsNull<T> extends WithParams <T> {
	@NameCn("输出结果列列名数组")
	@DescCn("输出结果列列名数组，可选，默认null")
	ParamInfo <String[]> OUTPUT_COLS = ParamInfoFactory
		.createParamInfo("outputCols", String[].class)
		.setDescription("Names of the output columns")
		.setAlias(new String[] {"outputColNames"})
		.setHasDefaultValue(null)
		.build();

	default String[] getOutputCols() {
		return get(OUTPUT_COLS);
	}

	default T setOutputCols(String... colNames) {
		return set(OUTPUT_COLS, colNames);
	}
}
