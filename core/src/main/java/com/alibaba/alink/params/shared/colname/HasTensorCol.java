package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasTensorCol<T> extends WithParams <T> {

	@NameCn("tensor列")
	@DescCn("tensor列")
	ParamInfo <String> TENSOR_COL = ParamInfoFactory
		.createParamInfo("tensorCol", String.class)
		.setDescription("tensor column")
		.setRequired()
		.build();

	default String getTensorCol() {
		return get(TENSOR_COL);
	}

	default T setTensorCol(String value) {
		return set(TENSOR_COL, value);
	}
}
