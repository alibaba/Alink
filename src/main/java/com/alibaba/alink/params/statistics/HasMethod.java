package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Parameter of correlation method.
 */
public interface HasMethod<T> extends WithParams<T> {

    ParamInfo<String> METHOD = ParamInfoFactory
        .createParamInfo("method", String.class)
        .setDescription("method: pearson, spearman. default pearson")
        .setHasDefaultValue("pearson")
        .build();

    default String getMethod() {
        return get(METHOD);
    }

    default T setMethod(String value) {
        return set(METHOD, value);
    }

}
