package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasZipCols<T> extends WithParams <T> {
	@NameCn("Zipped 列列名数组")
	@DescCn("Zipped 列列名数组")
	ParamInfo <String[]> ZIPPED_COLS = ParamInfoFactory
		.createParamInfo("zippedCols", String[].class)
		.setDescription("zipped column names")
		//.setRequired()
		.build();

	default String[] getZippedCols() {
		return get(ZIPPED_COLS);
	}

	default T setZippedCols(String... value) {
		return set(ZIPPED_COLS, value);
	}
}
