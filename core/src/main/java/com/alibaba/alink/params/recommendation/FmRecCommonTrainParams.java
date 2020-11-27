package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface FmRecCommonTrainParams<T> extends
        HasUserCol<T>,
        HasItemCol<T>,
        FmCommonTrainParams<T> {

    ParamInfo<String[]> USER_FEATURE_COLS = ParamInfoFactory
            .createParamInfo("userFeatureCols", String[].class)
            .setDescription("")
            .setHasDefaultValue(new String[0])
            .build();

    ParamInfo<String[]> USER_CATEGORICAL_FEATURE_COLS = ParamInfoFactory
            .createParamInfo("userCategoricalFeatureCols", String[].class)
            .setDescription("")
            .setHasDefaultValue(new String[0])
            .build();

    ParamInfo<String[]> ITEM_FEATURE_COLS = ParamInfoFactory
            .createParamInfo("itemFeatureCols", String[].class)
            .setDescription("")
            .setHasDefaultValue(new String[0])
            .build();

    ParamInfo<String[]> ITEM_CATEGORICAL_FEATURE_COLS = ParamInfoFactory
            .createParamInfo("itemCategoricalFeatureCols", String[].class)
            .setDescription("")
            .setHasDefaultValue(new String[0])
            .build();

    default T setUserFeatureCols(String[] value) {
        return set(USER_FEATURE_COLS, value);
    }

    default T setUserCategoricalFeatureCols(String[] value) {
        return set(USER_CATEGORICAL_FEATURE_COLS, value);
    }

    default T setItemFeatureCols(String[] value) {
        return set(ITEM_FEATURE_COLS, value);
    }

    default T setItemCategoricalFeatureCols(String[] value) {
        return set(ITEM_CATEGORICAL_FEATURE_COLS, value);
    }

    default String[] getUserFeatureCols() {
        return get(USER_FEATURE_COLS);
    }

    default String[] getUserCategoricalFeatureCols() {
        return get(USER_CATEGORICAL_FEATURE_COLS);
    }

    default String[] setItemFeatureCols() {
        return get(ITEM_FEATURE_COLS);
    }

    default String[] setItemCategoricalFeatureCols() {
        return get(ITEM_CATEGORICAL_FEATURE_COLS);
    }
}
