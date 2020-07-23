package com.alibaba.alink.params.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.common.evaluation.TuningClusterMetric;
import com.alibaba.alink.params.ParamUtil;

public interface HasTuningClusterMetric<T> extends WithParams<T> {
    ParamInfo<TuningClusterMetric> TUNING_CLUSTER_METRIC = ParamInfoFactory
        .createParamInfo("tuningClusterMetric", TuningClusterMetric.class)
        .setDescription("metric of cluster evaluation in tuning")
        .setRequired()
        .build();

    default TuningClusterMetric getTuningClusterMetric() {
        return get(TUNING_CLUSTER_METRIC);
    }

    default T setTuningClusterMetric(TuningClusterMetric metric) {
        return set(TUNING_CLUSTER_METRIC, metric);
    }

    default T setTuningClusterMetric(String metric) {
        return set(TUNING_CLUSTER_METRIC, ParamUtil.searchEnum(TUNING_CLUSTER_METRIC, metric));
    }
}
