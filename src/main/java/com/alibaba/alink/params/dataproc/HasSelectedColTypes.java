package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasSelectedColTypes<T> extends WithParams<T> {
    ParamInfo<String[]> SELECTED_COL_TYPES = ParamInfoFactory
        .createParamInfo("selectedColTypes", String[].class)
        .setRequired()
        .build();
}
