package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * FeatureType.
 */
public interface HasFeatureType<T> extends WithParams<T> {
    ParamInfo<String> FEATURE_TYPE = ParamInfoFactory
        .createParamInfo("featureType", String.class)
        .setDescription("Feature type, support IDF/WORD_COUNT/TF_IDF/Binary/TF")
        .setHasDefaultValue("WORD_COUNT")
        .build();

    default String getFeatureType() {
        return get(FEATURE_TYPE);
    }

    default T setFeatureType(String value) {
        return set(FEATURE_TYPE, value);
    }
}
