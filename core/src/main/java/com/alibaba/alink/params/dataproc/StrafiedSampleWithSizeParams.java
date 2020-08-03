package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasGroupCol;
import com.alibaba.alink.params.validators.RangeValidator;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface StrafiedSampleWithSizeParams<T> extends
    HashWithReplacementParams<T>,
    HasGroupCol<T> {
    ParamInfo<Integer> SIZE = ParamInfoFactory
        .createParamInfo("size", Integer.class)
        .setDescription("sampling size")
        .setHasDefaultValue(null)
        .setValidator(new RangeValidator<>(0, Integer.MAX_VALUE))
        .build();

    default Integer getSize() {
        return getParams().get(SIZE);
    }

    default T setSize(Integer value) {
        return set(SIZE, value);
    }

    ParamInfo<String> SIZES = ParamInfoFactory
        .createParamInfo("sizes", String.class)
        .setDescription(
            "sampling size, it should be int value when as a number , defintion format as name1:number1,name2:number2"
                + " when as a string")
        .setHasDefaultValue(null)
        .build();

    default String getSizes() {
        return getParams().get(SIZES);
    }

    default T setSizes(String value) {
        return set(SIZES, value);
    }
}
