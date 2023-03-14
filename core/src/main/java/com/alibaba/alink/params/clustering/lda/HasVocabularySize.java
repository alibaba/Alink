package com.alibaba.alink.params.clustering.lda;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * vocabulary size.
 */
public interface HasVocabularySize<T> extends WithParams <T> {

	/**
	 * @cn 文章的超参
	 */
	@NameCn("词汇大小")
	@DescCn("文章的超参")
	ParamInfo <Integer> VOCABULARY_SIZE = ParamInfoFactory
		.createParamInfo("vocabularySize", Integer.class)
		.setDescription("vocabulary Size.")
		.setRequired()
		.build();

	default Integer getVocabularySize() {
		return get(VOCABULARY_SIZE);
	}

	default T setVocabularySize(Integer value) {
		return set(VOCABULARY_SIZE, value);
	}
}

