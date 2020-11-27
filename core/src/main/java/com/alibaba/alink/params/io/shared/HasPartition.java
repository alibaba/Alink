package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPartition<T> extends WithParams <T> {
	ParamInfo <String> PARTITION = ParamInfoFactory
		.createParamInfo("partition", String.class)
		.setDescription("partition")
		.setHasDefaultValue(null)
		.build();

	default String getPartition() {
		return get(PARTITION);
	}

	default T setPartition(String value) {
		return set(PARTITION, value);
	}
}
