package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasOutputCol;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Params for autocross.
 */
public interface AutoCrossPredictParams<T> extends HasOutputCol<T> {

    ParamInfo<Integer> NUM_CROSS_FEATURES = ParamInfoFactory
        .createParamInfo("numCrossFeatures", Integer.class)
        .setDescription("Number of cross features to generate.")
        .setRequired()
        .build();

    default Integer getNumCrossFeatures() {
        return get(NUM_CROSS_FEATURES);
    }

    default T setNumCrossFeatures(Integer value) {
        return set(NUM_CROSS_FEATURES, value);
    }
}
