package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Trait for parameter max.
 * It is Upper bound after transformation.
 */
public interface HasMax<T> extends WithParams<T> {

    ParamInfo<Double> MAX = ParamInfoFactory
        .createParamInfo("max", Double.class)
        .setDescription("Upper bound after transformation.")
        .setHasDefaultValue(1.0)
        .build();

    default Double getMax() {
        return get(MAX);
    }

    default T setMax(Double value) {
        return set(MAX, value);
    }
}
