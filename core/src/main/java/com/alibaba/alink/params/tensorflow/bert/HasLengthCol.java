package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasLengthCol<T> extends WithParams <T> {
	@NameCn("分词后序列长度的列名")
	@DescCn("分词后序列长度的列名")
	ParamInfo <String> LENGTH_COL = ParamInfoFactory
		.createParamInfo("lengthCol", String.class)
		.setDescription("Name of the length column")
		.setHasDefaultValue("length")
		.build();

	default String getLengthCol() {
		return get(LENGTH_COL);
	}

	default T setLengthCol(String colName) {
		return set(LENGTH_COL, colName);
	}
}
