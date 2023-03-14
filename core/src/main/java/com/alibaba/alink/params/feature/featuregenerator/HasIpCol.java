package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasIpCol<T> extends WithParams<T> {
    ParamInfo<String> IP_COL = ParamInfoFactory
        .createParamInfo("ipCol", String.class)
        .setDescription("ip col.")
        .setRequired()
        .build();

    default String getIpCol() {
        return get(IP_COL);
    }

    default T setIpCol(String value) {
        return set(IP_COL, value);
    }

}
