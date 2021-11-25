package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasBertModelName<T> extends WithParams <T> {
	/**
	 * @cn BERT模型名字： Base-Chinese,Base-Multilingual-Cased,Base-Uncased,Base-Cased
	 * @cn-name BERT模型名字
	 */
	ParamInfo <String> BERT_MODEL_NAME = ParamInfoFactory
		.createParamInfo("bertModelName", String.class)
		.setDescription("BERT model name: Base-Chinese,Base-Multilingual-Cased,Base-Uncased,Base-Cased")
		.setHasDefaultValue("Base-Chinese")
		.build();

	default String getBertModelName() {
		return get(BERT_MODEL_NAME);
	}

	default T setBertModelName(String value) {
		return set(BERT_MODEL_NAME, value);
	}
}
