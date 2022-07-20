package com.alibaba.alink.params.outlier;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasInputMTableCol<T> extends WithParams <T> {

	@NameCn("输入列名")
	@DescCn("输入序列的列名")
	ParamInfo <String> INPUT_MTABLE_COL = ParamInfoFactory
		.createParamInfo("inputMTableCol", String.class)
		.setDescription("The column name of input series in MTable type.")
		.setRequired()
		.build();

	default String getInputMTableCol() {
		return get(INPUT_MTABLE_COL);
	}

	default T setInputMTableCol(String colName) {
		return set(INPUT_MTABLE_COL, colName);
	}
}
