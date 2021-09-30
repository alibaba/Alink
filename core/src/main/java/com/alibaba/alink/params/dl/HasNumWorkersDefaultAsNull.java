package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumWorkersDefaultAsNull<T> extends WithParams<T> {


    ParamInfo<Integer> NUM_WORKERS = ParamInfoFactory
        .createParamInfo("numWorkers", Integer.class)
        .setDescription("number of workers")
        .setHasDefaultValue(null)
        .build();

    default Integer getNumWorkers() {
        return get(NUM_WORKERS);
    }

    default T setNumWorkers(Integer value) {
        return set(NUM_WORKERS, value);
    }
}
