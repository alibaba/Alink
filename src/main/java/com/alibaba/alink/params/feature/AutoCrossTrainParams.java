package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Params for autocross.
 */
public interface AutoCrossTrainParams<T> extends
    HasSelectedCols<T>, HasLabelCol<T> {

    ParamInfo<Integer> MAX_SEARCH_STEP = ParamInfoFactory
        .createParamInfo("maxSearchStep", Integer.class)
        .setDescription("Max search step.")
        .setHasDefaultValue(4)
        .build();

    default Integer getMaxSearchStep() {
        return get(MAX_SEARCH_STEP);
    }

    default T setMaxSearchStep(Integer value) {
        return set(MAX_SEARCH_STEP, value);
    }
}
