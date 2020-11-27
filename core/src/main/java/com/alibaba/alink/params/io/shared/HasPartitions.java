package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPartitions<T> extends WithParams <T> {

	ParamInfo <String> PARTITIONS = ParamInfoFactory
		.createParamInfo("partitions", String.class)
		.setDescription("partitions")
		.setHasDefaultValue(null)
		.build();

	default String getPartitions() {
		return get(PARTITIONS);
	}

	default T setPartitions(String value) {
		return set(PARTITIONS, value);
	}
}
