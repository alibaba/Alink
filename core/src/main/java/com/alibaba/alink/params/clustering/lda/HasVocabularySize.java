package com.alibaba.alink.params.clustering.lda;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * vocabulary size.
 */
public interface HasVocabularySize<T> extends WithParams<T> {
    ParamInfo<Integer> VOCABULARY_SIZE = ParamInfoFactory
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

