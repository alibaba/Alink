package com.alibaba.alink.params.udf;

import org.apache.flink.ml.api.misc.param.WithParams;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * @author dota.zk
 * @date 25/06/2019
 */
public interface HasClassName<T> extends WithParams<T> {
    ParamInfo<String> CLASS_NAME = ParamInfoFactory
        .createParamInfo("className", String.class)
        .setDescription("the name of udf class")
        .setRequired()
        .build();


    default T setClassName(String clsName) {
        return set(CLASS_NAME, clsName);
    }

    default String getClassName() {
        return get(CLASS_NAME);
    }
}
