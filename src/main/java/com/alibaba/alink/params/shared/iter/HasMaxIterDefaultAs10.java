package com.alibaba.alink.params.shared.iter;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMaxIterDefaultAs10<T> extends WithParams<T> {

    ParamInfo<Integer> MAX_ITER = ParamInfoFactory
        .createParamInfo("maxIter", Integer.class)
        .setDescription("Maximum iterations, The default value is 10")
        .setHasDefaultValue(10)
        .setAlias(new String[]{"maxIteration", "numIter"})
        .build();

    default Integer getMaxIter() {
        return get(MAX_ITER);
    }

    default T setMaxIter(Integer value) {
        return set(MAX_ITER, value);
    }
}
