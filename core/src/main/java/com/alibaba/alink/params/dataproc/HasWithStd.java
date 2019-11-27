package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Trait for parameter withStd.
 * If true, Scales the data to unit standard deviation.
 */
public interface HasWithStd<T> extends WithParams<T> {

    ParamInfo<Boolean> WITH_STD = ParamInfoFactory
        .createParamInfo("withStd", Boolean.class)
        .setDescription("Scales the data to unit standard deviation. true by default")
        .setHasDefaultValue(true)
        .build();

    default Boolean getWithStd() {
        return get(WITH_STD);
    }

    default T setWithStd(Boolean value) {
        return set(WITH_STD, value);
    }
}
