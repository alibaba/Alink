package com.alibaba.alink.params.feature.featuregenerator;

import com.alibaba.alink.params.ParamUtil;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasCalcType<T> extends WithParams<T> {
    ParamInfo<CalcType> CALC_TYPE = ParamInfoFactory
        .createParamInfo("calcType", CalcType.class)
        .setDescription("calc type")
        .setHasDefaultValue(HasCalcType.CalcType.COUNT)
        .build();

    default CalcType getCalcType() {
        return get(CALC_TYPE);
    }

    default T setCalcType(CalcType colNames) {
        return set(CALC_TYPE, colNames);
    }

    default T setCalcType(String value) {
        return set(CALC_TYPE, ParamUtil.searchEnum(CALC_TYPE, value));
    }

    enum CalcType {
        COUNT,
        SUM,
        MEAN,
        AVERAGE,
        DISTINCT_COUNT
    }
}
