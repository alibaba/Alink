package com.alibaba.alink.params.udf;

import org.apache.flink.ml.api.misc.param.WithParams;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * @author dota.zk
 * @date 25/06/2019
 */
public interface HasResultType<T> extends WithParams<T> {
    ParamInfo<String> RESULT_TYPE = ParamInfoFactory
        .createParamInfo("resultType", String.class)
        .setDescription("the type of result, should be one of {'BOOLEAN', 'INTEGER', 'LONG', 'FLOAT', 'DOUBLE', 'STRING'}")
        .setRequired()
        .build();

    default T setResultType(String resultType) {
        return set(RESULT_TYPE, resultType);
    }

    default String getResultType() {
        return get(RESULT_TYPE);
    }
}
