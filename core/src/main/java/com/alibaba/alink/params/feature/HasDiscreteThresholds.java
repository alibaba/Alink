package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasDiscreteThresholds<T> extends WithParams<T> {

    ParamInfo<Integer> DISCRETE_THRESHOLDS = ParamInfoFactory
        .createParamInfo("discreteThresholds", Integer.class)
        .setDescription("discreteThreshold")
        .setAlias(new String[]{"discreteThreshold"})
        .setHasDefaultValue(Integer.MIN_VALUE)
        .build();

    default Integer getDiscreteThresholds() {
        return get(DISCRETE_THRESHOLDS);
    }

    default T setDiscreteThresholds(Integer value) {
        return set(DISCRETE_THRESHOLDS, value);
    }

}
