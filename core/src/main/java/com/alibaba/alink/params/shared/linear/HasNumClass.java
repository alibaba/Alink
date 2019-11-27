package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * num class of multi class train.
 *
 */
public interface HasNumClass<T> extends WithParams<T> {
    ParamInfo<Integer> NUM_CLASS = ParamInfoFactory
        .createParamInfo("numClass", Integer.class)
        .setDescription("num class of multi class train.")
        .setRequired()
        .setAlias(new String[] {"nClass"})
        .build();

    default Integer getNumClass() {
        return get(NUM_CLASS);
    }

    default T setNumClass(Integer value) {
        return set(NUM_CLASS, value);
    }
}
