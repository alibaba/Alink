package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.dataproc.HasWithMean;
import com.alibaba.alink.params.dataproc.HasWithStd;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * Trait for parameter PcaTrain.
 */
public interface PcaTrainParams<T> extends
        HasSelectedColsDefaultAsNull<T>,
        HasVectorColDefaultAsNull<T>, HasWithMean<T>, HasWithStd<T> {
    ParamInfo<Integer> K = ParamInfoFactory
            .createParamInfo("k", Integer.class)
            .setDescription("the value of K.")
            .setRequired()
            .setAlias(new String[]{"p"})
            .build();
    ParamInfo<String> CALCULATION_TYPE = ParamInfoFactory
            .createParamInfo("calculationType", String.class)
            .setDescription("compute type, be CORR, COV_SAMPLE, COVAR_POP.")
            .setHasDefaultValue("CORR")
            .setAlias(new String[]{"calcType", "pcaType"})
            .build();

    default Integer getK() {
        return get(K);
    }

    default T setK(Integer value) {
        return set(K, value);
    }

    default String getCalculationType() {
        return get(CALCULATION_TYPE);
    }

    default T setCalculationType(String value) {
        return set(CALCULATION_TYPE, value);
    }
}
