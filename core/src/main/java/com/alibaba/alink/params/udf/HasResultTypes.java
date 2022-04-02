package com.alibaba.alink.params.udf;

import org.apache.flink.ml.api.misc.param.WithParams;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * @author dota.zk
 * @date 25/06/2019
 */
public interface HasResultTypes<T> extends WithParams<T> {
    ParamInfo<String[]> RESULT_TYPE = ParamInfoFactory
        .createParamInfo("resultTypes", String[].class)
        .setDescription("the type list of result, each should be one of {'BOOLEAN', 'INTEGER', 'LONG', 'FLOAT', 'DOUBLE', 'STRING'}")
        .setRequired()
        .build();

    default T setResultTypes(String... resultType) {
        return set(RESULT_TYPE, resultType);
    }

    default String[] getResultTypes() {
        return get(RESULT_TYPE);
    }
}
