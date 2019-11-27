package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface AlsRecPredictParams<T>
    extends WithParams<T> {

    ParamInfo<Integer> TOP_K = ParamInfoFactory
        .createParamInfo("topK", Integer.class)
        .setDescription("Number of items recommended")
        .setHasDefaultValue(100)
        .build();

    default Integer getTopK() {
        return get(TOP_K);
    }

    default T setTopK(Integer value) {
        return set(TOP_K, value);
    }

}
