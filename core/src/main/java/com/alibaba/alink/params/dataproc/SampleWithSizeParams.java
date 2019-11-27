package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface SampleWithSizeParams<T> extends
    WithParams<T> {

    ParamInfo<Integer> SIZE = ParamInfoFactory
        .createParamInfo("size", Integer.class)
        .setDescription("sampling size")
        .setRequired()
        .build();

    ParamInfo<Boolean> WITH_REPLACEMENT = ParamInfoFactory
        .createParamInfo("withReplacement", Boolean.class)
        .setDescription("Indicates whether to enable sampling with replacement, default is without replcement")
        .setHasDefaultValue(false)
        .build();


    default Integer getSize() {
        return getParams().get(SIZE);
    }

    default T setSize(Integer value) {
        return set(SIZE, value);
    }

    default Boolean getWithReplacement() {
        return getParams().get(WITH_REPLACEMENT);
    }

    default T setWithReplacement(Boolean value) {
        return set(WITH_REPLACEMENT, value);
    }

}
