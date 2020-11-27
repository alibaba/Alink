package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.shared.linear.HasEpsilonDv0000001;

/**
 * parameters of fm trainer.
 */
public interface FmTrainParams<T> extends
        HasLabelCol<T>,
        HasVectorColDefaultAsNull<T>,
        HasWeightColDefaultAsNull<T>,
        HasEpsilonDv0000001<T>,
        HasFeatureColsDefaultAsNull<T>,
        FmCommonTrainParams<T> {

    ParamInfo<Integer> MINIBATCH_SIZE = ParamInfoFactory
            .createParamInfo("minibatchSize", Integer.class)
            .setDescription("mini-batch size")
            .setHasDefaultValue(-1)
            .build();

    default Integer getBatchSize() {
        return get(MINIBATCH_SIZE);
    }

    default T setBatchSize(Integer value) {
        return set(MINIBATCH_SIZE, value);
    }
}
