package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasK<T> extends WithParams<T> {

    ParamInfo<Integer> K = ParamInfoFactory
            .createParamInfo("k", Integer.class)
            .setDescription("the value of K.")
            .setRequired()
            .setAlias(new String[]{"p"})
            .build();

    default Integer getK() {
        return get(K);
    }

    default T setK(Integer value) {
        return set(K, value);
    }
}