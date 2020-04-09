package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.ParamUtil;
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

    ParamInfo<TransformType> TRANSFORM_TYPE = ParamInfoFactory
        .createParamInfo("transformType", TransformType.class)
        .setDescription("'SIMPLE' or 'SUBMEAN', SIMPLE is data * model, SUBMEAN is (data - mean) * model")
        .setHasDefaultValue(TransformType.SIMPLE)
        .build();

    default TransformType getTransformType() {
        return get(TRANSFORM_TYPE);
    }

    default T setTransformType(TransformType value) {
        return set(TRANSFORM_TYPE, value);
    }

    default T setTransformType(String value) {
        return set(TRANSFORM_TYPE, ParamUtil.searchEnum(TRANSFORM_TYPE, value));
    }

    /**
     * pca transform type.
     */
    enum TransformType {
        /**
         *  data * model
         */
        SIMPLE,

        /**
         * (data - mean) * model
         */
        SUBMEAN,

        /**
         * (data - mean) / stdVar * model
         */
        NORMALIZATION
    }
}
