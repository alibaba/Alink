package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxSeqLength<T> extends WithParams <T> {
	@NameCn("句子截断长度")
	@DescCn("句子截断长度")
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
