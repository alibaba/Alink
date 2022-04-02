package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * The maximum word number of the dictionary.
 */
public interface HasVocabSize<T> extends WithParams <T> {
	@NameCn("字典库大小")
	@DescCn("字典库大小，如果总词数目大于这个值，那个文档频率低的词会被过滤掉。")
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
