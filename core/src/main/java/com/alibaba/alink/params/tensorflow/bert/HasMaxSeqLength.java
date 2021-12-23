package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMaxSeqLength<T> extends WithParams <T> {
	/**
	 * @cn 句子截断长度
	 * @cn-name 句子截断长度
	 */
	ParamInfo <Integer> MAX_SEQ_LENGTH = ParamInfoFactory
		.createParamInfo("maxSeqLength", Integer.class)
		.setDescription("maxSeqLength")
		.setHasDefaultValue(128)
		.build();

	default Integer getMaxSeqLength() {
		return get(MAX_SEQ_LENGTH);
	}

	default T setMaxSeqLength(Integer value) {
		return set(MAX_SEQ_LENGTH, value);
	}
}
