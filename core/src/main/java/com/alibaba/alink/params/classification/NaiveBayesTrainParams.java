package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.colname.*;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Parameters of naive bayes train process.
 */
public interface NaiveBayesTrainParams<T> extends
    HasCategoricalCols<T>,
    HasFeatureCols<T>,
    HasLabelCol<T>,
    HasWeightColDefaultAsNull<T>{

    ParamInfo <Double> SMOOTHING = ParamInfoFactory
        .createParamInfo("smoothing", Double.class)
        .setDescription("the smoothing factor")
        .setHasDefaultValue(0.0)
        .build();

    default Double getSmoothing() {
        return get(SMOOTHING);
    }

    default T setSmoothing(Double value) {
        return set(SMOOTHING, value);
    }
}
