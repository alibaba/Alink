package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumWorkersDefaultAsNull<T> extends WithParams<T> {

    /**
     * @cn Worker 角色的数量。值未设置时，如果 PS 角色数也未设置，则为作业总并发度的 3/4（需要取整），否则为总并发度减去 PS 角色数。
     * @cn-name Worker 角色数
     */
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
