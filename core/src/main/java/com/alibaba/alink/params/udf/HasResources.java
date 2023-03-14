package com.alibaba.alink.params.udf;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * @author dota.zk
 * @date 25/06/2019
 */
public interface HasResources<T> extends WithParams<T> {
    @NameCn("资源位置")
    @DescCn("资源位置")
    ParamInfo<String[]> RESOURCES = ParamInfoFactory
        .createParamInfo("resources", String[].class)
        .setDescription("the location of resource")
        .setRequired()
        .build();

    default String[] getResources() {
        return get(RESOURCES);
    }

    default T setResources(String... value) {
        return set(RESOURCES, value);
    }
}
