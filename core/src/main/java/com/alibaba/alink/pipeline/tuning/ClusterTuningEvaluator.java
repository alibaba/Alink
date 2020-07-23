package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.ml.api.misc.param.ParamInfo;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp;
import com.alibaba.alink.operator.common.evaluation.TuningClusterMetric;
import com.alibaba.alink.params.evaluation.EvalClusterParams;
import com.alibaba.alink.params.evaluation.HasTuningClusterMetric;

public class ClusterTuningEvaluator extends TuningEvaluator<ClusterTuningEvaluator>
    implements EvalClusterParams<ClusterTuningEvaluator>,
    HasTuningClusterMetric<ClusterTuningEvaluator> {

    public ClusterTuningEvaluator() {
        super(null);
    }

    @Override
    public double evaluate(BatchOperator in) {
        return new EvalClusterBatchOp(getParams())
            .linkFrom(in)
            .collectMetrics()
            .getParams()
            .get(getMetricParamInfo());
    }

    @Override
    public boolean isLargerBetter() {
        return getTuningClusterMetric().equals(TuningClusterMetric.SP)
            || getTuningClusterMetric().equals(TuningClusterMetric.VRC)
            || getTuningClusterMetric().equals(TuningClusterMetric.SSB);
    }

    @Override
    ParamInfo<Double> getMetricParamInfo() {
        return getTuningClusterMetric().getMetricKey();
    }
}
