package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public class HasModelName {
	public final ParamInfo <String> MODEL_NAME = ParamInfoFactory
		.createParamInfo("modelName", String.class)
		.setDescription("Name of the model")
		.setRequired()
		.build();
}
