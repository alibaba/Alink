package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Trait for parameter withMean.
 * If true, center the data with mean before scaling.
 */
public interface HasWithMean<T> extends WithParams<T> {

    ParamInfo<Boolean> WITH_MEAN = ParamInfoFactory
        .createParamInfo("withMean", Boolean.class)
        .setDescription("Centers the data with mean before scaling.")
        .setHasDefaultValue(true)
        .build();

    default Boolean getWithMean() {
        return get(WITH_MEAN);
    }

    default T setWithMean(Boolean value) {
        return set(WITH_MEAN, value);
    }
}
