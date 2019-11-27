package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp;
import com.alibaba.alink.operator.common.evaluation.BaseSimpleClusterMetrics;
import com.alibaba.alink.params.evaluation.EvalClusterParams;
import org.apache.flink.ml.api.misc.param.ParamInfo;

public class ClusterTuningEvaluator extends TuningEvaluator <ClusterTuningEvaluator>
    implements EvalClusterParams <ClusterTuningEvaluator> {

    public ClusterTuningEvaluator() {
        super(null);
    }

    @Override
    public double evaluate(BatchOperator in) {
        return (double) new EvalClusterBatchOp(getParams())
            .linkFrom(in)
            .collectMetrics()
            .getParams()
            .get(findParamInfo(BaseSimpleClusterMetrics.class, getMetricName()));
    }

    @Override
    public boolean isLargerBetter() {
        ParamInfo paramInfo = findParamInfo(BaseSimpleClusterMetrics.class, getMetricName());
        return (paramInfo.equals(BaseSimpleClusterMetrics.SEPERATION)
            || paramInfo.equals(BaseSimpleClusterMetrics.CALINSKI_HARABAZ));
    }
}
