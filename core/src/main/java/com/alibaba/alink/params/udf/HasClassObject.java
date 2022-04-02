package com.alibaba.alink.params.udf;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasClassObject<T> extends WithParams<T> {
    ParamInfo<String> CLASS_OBJECT = ParamInfoFactory
        .createParamInfo("classObject", String.class)
        .setDescription("the binary content of udf class")
        .setRequired()
        .build();


    default T setClassObject(String clsName) {
        return set(CLASS_OBJECT, clsName);
    }

    default String getClassObject() {
        return get(CLASS_OBJECT);
    }
}
