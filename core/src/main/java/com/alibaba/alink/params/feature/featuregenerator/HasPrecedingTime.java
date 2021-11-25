package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPrecedingTime<T> extends WithParams<T> {

    /**
     * @cn-name 时间窗口大小
     * @cn 时间窗口大小
     */
    ParamInfo<Double> PRECEDING_TIME = ParamInfoFactory
        .createParamInfo("precedingTime", Double.class)
        .setDescription("time interval")
        .setHasDefaultValue(null)
        .build();

    default Double getPrecedingTime() {return get(PRECEDING_TIME);}

    default T setPrecedingTime(Double value) {return set(PRECEDING_TIME, value);}

    default T setPrecedingTime(int value) {return set(PRECEDING_TIME, (double) value);}

}
