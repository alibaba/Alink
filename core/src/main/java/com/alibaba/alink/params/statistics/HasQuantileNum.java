package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Quantile Num.
 */
public interface HasQuantileNum<T> extends WithParams<T> {

    ParamInfo<Integer> QUANTILE_NUM = ParamInfoFactory
        .createParamInfo("quantileNum", Integer.class)
        .setDescription("quantile num")
        .setRequired()
        .setAlias(new String[]{"N"})
        .build();

    default Integer getQuantileNum() {
        return get(QUANTILE_NUM);
    }

    default T setQuantileNum(Integer value) {
        return set(QUANTILE_NUM, value);
    }
}
