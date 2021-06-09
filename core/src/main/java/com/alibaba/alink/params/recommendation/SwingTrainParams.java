package com.alibaba.alink.params.recommendation;


import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface SwingTrainParams<T> extends
    CommonSwingParams<T> {

    ParamInfo<Float> USER_ALPHA = ParamInfoFactory
        .createParamInfo("userAlpha", Float.class)
        .setHasDefaultValue(5.F)
        .build();

    default Float getUserAlpha() {
        return get(USER_ALPHA);
    }

    default T setUserAlpha(Double alpha) {
        return set(USER_ALPHA, alpha.floatValue());
    }

    default T setUserAlpha(Integer alpha) {
        return set(USER_ALPHA, alpha.floatValue());
    }

    ParamInfo<Float> USER_BETA = ParamInfoFactory
        .createParamInfo("userBeta", Float.class)
        .setHasDefaultValue(-0.35F)
        .build();

    default Float getUserBeta() {
        return get(USER_BETA);
    }

    default T setUserBeta(Double alpha) {
        return set(USER_BETA, alpha.floatValue());
    }

    default T setUserBeta(Integer alpha) {
        return set(USER_BETA, alpha.floatValue());
    }

}
