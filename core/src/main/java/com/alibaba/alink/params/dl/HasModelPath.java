package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasModelPath<T> extends WithParams <T> {

	@NameCn("模型的URL路径")
	@DescCn("模型的URL路径")
	ParamInfo <String> MODEL_PATH = ParamInfoFactory
		.createParamInfo("modelPath", String.class)
		.setDescription("model path")
		.setRequired()
		.build();

	default String getModelPath() {
		return get(MODEL_PATH);
	}

	default T setModelPath(String value) {
		return set(MODEL_PATH, value);
	}
}
