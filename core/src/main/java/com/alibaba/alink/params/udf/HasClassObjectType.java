package com.alibaba.alink.params.udf;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasClassObjectType<T> extends WithParams<T> {
    ParamInfo<String> CLASS_OBJECT_TYPE = ParamInfoFactory
        .createParamInfo("classObjectType", String.class)
        .setDescription("the type of the binary content of udf class")
        .setRequired()
        .build();


    default T setClassObjectType(String clsName) {
        return set(CLASS_OBJECT_TYPE, clsName);
    }

    default String getClassObjectType() {
        return get(CLASS_OBJECT_TYPE);
    }
}
