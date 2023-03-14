package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasUserCol<T> extends WithParams<T> {
    ParamInfo<String> USER_COL = ParamInfoFactory
        .createParamInfo("userCol", String.class)
        .setDescription("user col.")
        .setRequired()
        .build();

    default String getUserCol() {
        return get(USER_COL);
    }

    default T setUserCol(String value) {
        return set(USER_COL, value);
    }
}
