package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasProperties<T> extends WithParams<T> {

    /**
     * @cn 用户自定义Kafka参数,形如: "prop1=val1,prop2=val2"
     * @cn-name 用户自定义Kafka参数
     */
    ParamInfo<String> PROPERTIES = ParamInfoFactory
        .createParamInfo("properties", String.class)
        .setDescription("user defined kafka properties, for example: \"prop1=val1,prop2=val2\"")
        .setHasDefaultValue(null)
        .build();

    default String getProperties() {
        return get(PROPERTIES);
    }

    default T setProperties(String value) {
        return set(PROPERTIES, value);
    }
}
