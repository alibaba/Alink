package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Minimum document frequency of a word.
 */
public interface HasMinTF<T> extends WithParams<T> {
    ParamInfo<Double> MIN_TF = ParamInfoFactory
        .createParamInfo("minTF", Double.class)
        .setDescription("When the number word in this document in is below minTF, the word will be ignored. It could be an exact "
            + "count or a fraction of the document token count. When minTF is within [0, 1), it's used as a fraction.")
        .setHasDefaultValue(1.0)
        .build();

    default double getMinTF() {
        return get(MIN_TF);
    }

    default T setMinTF(Double value) {
        return set(MIN_TF, value);
    }
}
