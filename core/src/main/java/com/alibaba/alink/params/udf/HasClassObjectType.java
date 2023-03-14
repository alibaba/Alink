package com.alibaba.alink.params.udf;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasClassObjectType<T> extends WithParams<T> {
    @NameCn("udf二进制文件的类型")
    @DescCn("udf二进制文件的类型")
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
