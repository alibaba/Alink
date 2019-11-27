package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface FirstNParams<T> extends
    WithParams<T> {

    ParamInfo<Integer> SIZE = ParamInfoFactory
        .createParamInfo("size", Integer.class)
        .setDescription("sampling size")
        .setRequired()
        .build();

    default Integer getSize() {
        return getParams().get(SIZE);
    }

    default T setSize(Integer value) {
        return set(SIZE, value);
    }
}
