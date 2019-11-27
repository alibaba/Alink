package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * Trait for parameter PcaPredict.
 */
public interface PcaPredictParams<T> extends
    HasReservedCols<T>,
    HasPredictionCol<T>,
    HasVectorColDefaultAsNull<T> {

    ParamInfo<String> TRANSFORM_TYPE = ParamInfoFactory
        .createParamInfo("transformType", String.class)
        .setDescription("'SIMPLE' or 'SUBMEAN', SIMPLE is data * model, SUBMEAN is (data - mean) * model")
        .setHasDefaultValue("SIMPLE")
        .build();

    default String getTransformType() {
        return get(TRANSFORM_TYPE);
    }

    default T setTransformType(String value) {
        return set(TRANSFORM_TYPE, value);
    }
}
