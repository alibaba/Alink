package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasGroupCol;
import com.alibaba.alink.params.validators.RangeValidator;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface StrafiedSampleParams<T> extends
        HasGroupCol<T>,
        HashWithReplacementParams<T>{
    ParamInfo<String> RATIOS = ParamInfoFactory
            .createParamInfo("ratios", String.class)
            .setDescription("sampling ratio, it should be in range of [0, 1] when as a number , "
                + "defintion format as name1:number1,name2:number2 when as a string")
            .setHasDefaultValue(null)
            .build();

    default String getRatios() {
        return getParams().get(RATIOS);
    }

    default T setRatios(String value) {
        return set(RATIOS, value);
    }

    ParamInfo<Double> RATIO = ParamInfoFactory
        .createParamInfo("ratio", Double.class)
        .setDescription("sampling ratio, it should be in range of [0, 1]")
        .setHasDefaultValue(null)
        .setValidator(new RangeValidator<>(0.0, 1.0))
        .build();

    default Double getRatio() {
        return getParams().get(RATIO);
    }

    default T setRatio(Double value) {
        return set(RATIO, value);
    }
}
