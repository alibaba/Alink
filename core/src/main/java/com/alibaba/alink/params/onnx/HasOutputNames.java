package com.alibaba.alink.params.onnx;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasOutputNames<T> extends WithParams <T> {

	@NameCn("ONNX 模型输出名")
	@DescCn("ONNX 模型输出名，用逗号分隔，并且与输出 Schema 一一对应，默认与输出 Schema 中的列名相同")
	ParamInfo <String[]> OUTPUT_NAMES = ParamInfoFactory
		.createParamInfo("outputNames", String[].class)
		.setDescription("ONNX model output names corresponding to the output schema string")
		.setHasDefaultValue(null)
		.build();

	default String[] getOutputNames() {
		return get(OUTPUT_NAMES);
	}

	default T setOutputNames(String... outputNames) {
		return set(OUTPUT_NAMES, outputNames);
	}
}
