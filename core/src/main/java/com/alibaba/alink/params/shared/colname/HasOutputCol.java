package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * An interface for classes with a parameter specifying the name of the output column.
 *
 * @see HasOutputCols
 * @see HasOutputColDefaultAsNull
 * @see HasOutputColsDefaultAsNull
 */
public interface HasOutputCol<T> extends WithParams <T> {
	@NameCn("输出结果列列名")
	@DescCn("输出结果列列名，必选")
	ParamInfo <String> OUTPUT_COL = ParamInfoFactory
		.createParamInfo("outputCol", String.class)
		.setDescription("Name of the output column")
		.setAlias(new String[] {"outputColName"})
		.setRequired()
		.build();

	default String getOutputCol() {
		return get(OUTPUT_COL);
	}

	default T setOutputCol(String colName) {
		return set(OUTPUT_COL, colName);
	}
}
