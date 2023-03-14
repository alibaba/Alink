package com.alibaba.alink.params.udf;

import org.apache.flink.ml.api.misc.param.WithParams;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * @author dota.zk
 * @date 25/06/2019
 */
public interface HasLanguage<T> extends WithParams<T> {
    @NameCn("语言")
    @DescCn("UDF的执行语言，可以是 java 或 python")
    ParamInfo<String> LANGUAGE = ParamInfoFactory
        .createParamInfo("language", String.class)
        .setDescription("the implemented lauange of this udf, should be java or python")
        .setRequired()
        .build();


    default T setLanguage(String clsName) {
        return set(LANGUAGE, clsName);
    }

    default String getLanguage() {
        return get(LANGUAGE);
    }
}
