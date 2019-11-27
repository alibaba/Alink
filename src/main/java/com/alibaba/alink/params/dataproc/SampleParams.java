package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.validators.RangeValidator;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface SampleParams<T> extends
    WithParams<T> {

    ParamInfo<Double> RATIO = ParamInfoFactory
        .createParamInfo("ratio", Double.class)
        .setDescription("sampling ratio, it should be in range of [0, 1]")
        .setRequired()
        .setValidator(new RangeValidator<>(0.0, 1.0))
        .build();

    ParamInfo<Boolean> WITH_REPLACEMENT = ParamInfoFactory
        .createParamInfo("withReplacement", Boolean.class)
        .setDescription("Indicates whether to enable sampling with replacement, default is without replcement")
        .setHasDefaultValue(false)
        .build();


    default Double getRatio() {
        return getParams().get(RATIO);
    }

    default T setRatio(Double value) {
        return set(RATIO, value);
    }

    default Boolean getWithReplacement() {
        return getParams().get(WITH_REPLACEMENT);
    }

    default T setWithReplacement(Boolean value) {
        return set(WITH_REPLACEMENT, value);
    }

}
