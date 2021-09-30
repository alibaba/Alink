package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface CommonSwingParams<T> extends
    HasUserCol<T>,
    HasItemCol<T> {

    ParamInfo<Float> ALPHA = ParamInfoFactory
        .createParamInfo("alpha", Float.class)
        .setDescription("Alpha.")
        .setHasDefaultValue(1.F)
        .build();

    default Float getAlpha() {
        return get(ALPHA);
    }

    default T setAlpha(Double alpha) {
        return set(ALPHA, alpha.floatValue());
    }

    default T setAlpha(Integer alpha) {
        return set(ALPHA, alpha.floatValue());
    }

    ParamInfo <String> RATE_COL = ParamInfoFactory
        .createParamInfo("rateCol", String.class)
        .setAlias(new String[] {"rateColName"})
        .setDescription("Rating column name")
        .setHasDefaultValue(null)
        .build();

    default String getRateCol() {
        return get(RATE_COL);
    }

    default T setRateCol(String value) {
        return set(RATE_COL, value);
    }
}
