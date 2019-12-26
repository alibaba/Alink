package com.alibaba.alink.params.udf;

import com.alibaba.alink.operator.common.utils.JoinType;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * join type used in UDTF operators, supports cross and leftOuter
 */
public interface HasJoinType<T> extends WithParams<T> {

    ParamInfo<String> JOIN_TYPE = ParamInfoFactory
        .createParamInfo("joinType", String.class)
        .setDescription("cross or leftOuter")
        .setHasDefaultValue(JoinType.CROSS.name())
        .build();


    default T setJoinType(String type) {
        return set(JOIN_TYPE, type.toUpperCase());
    }

    default String getJoinType() {
        return JoinType.valueOf(get(JOIN_TYPE)).name();
    }
}
