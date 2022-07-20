package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasOutputMTableCol<T> extends WithParams <T> {

	@NameCn("输出列名")
	@DescCn("输出序列的列名")
	ParamInfo <String> OUTPUT_MTABLE_COL = ParamInfoFactory
		.createParamInfo("outputMTableCol", String.class)
		.setDescription("The column name of output series in MTable type.")
		.setRequired()
		.build();

	default String getOutputMTableCol() {
		return get(OUTPUT_MTABLE_COL);
	}

	default T setOutputMTableCol(String colName) {
		return set(OUTPUT_MTABLE_COL, colName);
	}
}
