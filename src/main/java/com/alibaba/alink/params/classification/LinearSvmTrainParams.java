package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.linear.LinearTrainParams;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * parameters of linear svm training process.
 *
 */
public interface LinearSvmTrainParams<T> extends LinearTrainParams<T> {
    ParamInfo<Double> C = ParamInfoFactory
        .createParamInfo("C", Double.class)
        .setDescription("the penalty item parameter.")
        .setHasDefaultValue(1.0)
        .setAlias(new String[] {"C"})
        .build();

    default Double getC() {
        return get(C);
    }

    default T setC(Double value) {
        return set(C, value);
    }
}
