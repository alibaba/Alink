package com.alibaba.alink.params.onnx;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasInputNames<T> extends WithParams <T> {
	@NameCn("ONNX 模型输入名")
	@DescCn("ONNX 模型输入名，用逗号分隔，需要与输入列一一对应，默认与选择列相同")
	ParamInfo <String[]> INPUT_NAMES = ParamInfoFactory
		.createParamInfo("inputNames", String[].class)
		.setDescription("ONNX model input names corresponding to selected columns")
		.setHasDefaultValue(null)
		.build();

	default String[] getInputNames() {
		return get(INPUT_NAMES);
	}

	default T setInputNames(String... inputNames) {
		return set(INPUT_NAMES, inputNames);
	}
}
