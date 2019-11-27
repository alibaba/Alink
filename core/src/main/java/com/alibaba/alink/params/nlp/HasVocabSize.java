package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * The maximum word number of the dictionary.
 */
public interface HasVocabSize<T> extends WithParams <T> {
	ParamInfo <Integer> VOCAB_SIZE = ParamInfoFactory
		.createParamInfo("vocabSize", Integer.class)
		.setDescription("The maximum word number of the dictionary. If the total numbers of words are above this "
			+ "value,"
			+ "the words with lower document frequency will be filtered")
		.setHasDefaultValue(1 << 18)
		.build();

	default int getVocabSize() {
		return get(VOCAB_SIZE);
	}

	default T setVocabSize(Integer value) {
		return set(VOCAB_SIZE, value);
	}
}
