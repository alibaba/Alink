package com.alibaba.alink.params.linearprogramming;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import java.util.ArrayList;

public interface LPParams<T> extends WithParams<T>{
    ParamInfo <Integer> MAX_ITER = ParamInfoFactory
            .createParamInfo("maxIter", Integer.class)
            .setDescription("Maximum iterations, the default value is 50")
            .setHasDefaultValue(100)
            .setAlias(new String[] {"numIter"})
            .build();

    default Integer getMaxIter() {return get(MAX_ITER);}

    default T setMaxIter(Integer value) {return set(MAX_ITER, value);}
}
