package com.alibaba.alink.params.tensorflow.savedmodel;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasOutputNames<T> extends WithParams <T> {

	@NameCn("signature中的输出名")
	@DescCn("signature中的输出名，多个输出时用逗号分隔")
	ParamInfo <String[]> OUTPUT_NAMES = ParamInfoFactory
		.createParamInfo("outputNames", String[].class)
		.setDescription("output names")
		.build();

	default String[] getOutputNames() {
		return get(OUTPUT_NAMES);
	}

	default T setOutputNames(String[] value) {
		return set(OUTPUT_NAMES, value);
	}
}
