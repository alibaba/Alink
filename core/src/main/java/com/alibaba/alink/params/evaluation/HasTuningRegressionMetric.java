package com.alibaba.alink.params.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.common.evaluation.TuningRegressionMetric;
import com.alibaba.alink.params.ParamUtil;

public interface HasTuningRegressionMetric<T> extends WithParams<T> {
    ParamInfo<TuningRegressionMetric> TUNING_REGRESSION_METRIC = ParamInfoFactory
        .createParamInfo("tuningRegressionMetric", TuningRegressionMetric.class)
        .setDescription("metric of regression evaluation in tuning")
        .setRequired()
        .build();

    default TuningRegressionMetric getTuningRegressionMetric() {
        return get(TUNING_REGRESSION_METRIC);
    }

    default T setTuningRegressionMetric(TuningRegressionMetric metric) {
        return set(TUNING_REGRESSION_METRIC, metric);
    }

    default T setTuningRegressionMetric(String metric) {
        return set(TUNING_REGRESSION_METRIC, ParamUtil.searchEnum(TUNING_REGRESSION_METRIC, metric));
    }
}
