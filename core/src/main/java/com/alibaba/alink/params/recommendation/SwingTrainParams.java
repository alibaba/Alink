package com.alibaba.alink.params.recommendation;


import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface SwingTrainParams<T> extends
    CommonSwingParams<T> {

    /**
     * @cn-name 用户alpha参数
     * @cn 用户alpha参数，默认5.0, user weight = 1.0/(userAlpha + userClickCount)^userBeta
     */
    ParamInfo<Float> USER_ALPHA = ParamInfoFactory
        .createParamInfo("userAlpha", Float.class)
        .setDescription("user alpha")
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

    /**
     * @cn-name 用户beta参数
     * @cn 用户beta参数，默认-0.35, user weight = 1.0/(userAlpha + userClickCount)^userBeta
     */
    ParamInfo<Float> USER_BETA = ParamInfoFactory
        .createParamInfo("userBeta", Float.class)
        .setDescription("user beta")
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

    /**
     * @cn-name 结果是否归一化
     * @cn 是否归一化，默认False
     */
    ParamInfo<Boolean> RESULT_NORMALIZE = ParamInfoFactory
        .createParamInfo("resultNormalize", Boolean.class)
        .setHasDefaultValue(false)
        .build();

    default Boolean getResultNormalize() {
        return get(RESULT_NORMALIZE);
    }

    default T setResultNormalize(Boolean normalize) {
        return set(RESULT_NORMALIZE, normalize);
    }

    /**
     * @cn-name item参与计算的人数最大值
     * @cn 如果item出现次数大于该次数，会随机选择该次数的用户数据，默认1000
     */
    ParamInfo<Integer> MAX_ITEM_NUMBER = ParamInfoFactory
        .createParamInfo("maxItemNumber", Integer.class)
        .setDescription("max item number")
        .setHasDefaultValue(1000)
        .build();

    default Integer getMaxItemNumber() {
        return get(MAX_ITEM_NUMBER);
    }

    default T setMaxItemNumber(Integer item_frequency) {
        return set(MAX_ITEM_NUMBER, item_frequency);
    }

    /**
     * @cn-name 用户互动的最小Item数量
     * @cn 如果用户互动Item数量小于该次数，该用户数据不参与计算过程，默认10
     */
    ParamInfo<Integer> MIN_USER_ITEMS = ParamInfoFactory
        .createParamInfo("minUserItems", Integer.class)
        .setDescription("min user items")
        .setHasDefaultValue(10)
        .build();

    default Integer getMinUserItems() {
        return get(MIN_USER_ITEMS);
    }

    default T setMinUserItems(Integer minUserItems) {
        return set(MIN_USER_ITEMS, minUserItems);
    }

    /**
     * @cn-name 用户互动的最大Item数量
     * @cn 如果用户互动Item数量大于该次数，该用户数据不参与计算过程，默认1000
     */
    ParamInfo<Integer> MAX_USER_ITEMS = ParamInfoFactory
        .createParamInfo("maxUserItems", Integer.class)
        .setDescription("max user items")
        .setHasDefaultValue(1000)
        .build();

    default Integer getMaxUserItems() {
        return get(MAX_USER_ITEMS);
    }

    default T setMaxUserItems(Integer maxUserItems) {
        return set(MAX_USER_ITEMS, maxUserItems);
    }
}
