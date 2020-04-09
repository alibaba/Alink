package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasCalculationType<T> extends WithParams<T> {

    ParamInfo<CalculationType> CALCULATION_TYPE = ParamInfoFactory
        .createParamInfo("calculationType", CalculationType.class)
        .setDescription("compute type, be CORR, COV_SAMPLE, COVAR_POP.")
        .setHasDefaultValue(CalculationType.CORR)
        .setAlias(new String[]{"calcType", "pcaType"})
        .build();

    default CalculationType getCalculationType() {
        return get(CALCULATION_TYPE);
    }

    default T setCalculationType(CalculationType value) {
        return set(CALCULATION_TYPE, value);
    }

    default T setCalculationType(String value) {
        return set(CALCULATION_TYPE, ParamUtil.searchEnum(CALCULATION_TYPE, value));
    }

    /**
     * pca calculation type.
     */
    enum CalculationType {
        /**
         * correlation
         */
        CORR,
        /**
         * sample variance
         */
        COV_SAMPLE,

        /**
         *  population variance
         */
        COVAR_POP
    }
}
