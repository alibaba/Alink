package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp;
import com.alibaba.alink.operator.common.evaluation.TuningClusterMetric;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.evaluation.EvalClusterLocalOp;
import com.alibaba.alink.params.evaluation.EvalClusterParams;
import com.alibaba.alink.params.evaluation.HasTuningClusterMetric;

public class ClusterTuningEvaluator extends TuningEvaluator <ClusterTuningEvaluator>
	implements EvalClusterParams <ClusterTuningEvaluator>,
	HasTuningClusterMetric <ClusterTuningEvaluator> {

	public ClusterTuningEvaluator() {
		super(null);
	}

	public ClusterTuningEvaluator(Params params) {
		super(params);
	}

	@Override
	public double evaluate(BatchOperator <?> in) {
		return new EvalClusterBatchOp(getParams())
			.linkFrom(in)
			.collectMetrics()
			.getParams()
			.get(getMetricParamInfo());
	}

	@Override
	public double evaluate(LocalOperator <?> in) {
		return new EvalClusterLocalOp(getParams())
			.linkFrom(in)
			.collectMetrics()
			.getParams()
			.get(getMetricParamInfo());
	}

	@Override
	public boolean isLargerBetter() {
		return getTuningClusterMetric().equals(TuningClusterMetric.SP)
			|| getTuningClusterMetric().equals(TuningClusterMetric.SSB)
			|| getTuningClusterMetric().equals(TuningClusterMetric.VRC)
			|| getTuningClusterMetric().equals(TuningClusterMetric.SILHOUETTE_COEFFICIENT)
			|| getTuningClusterMetric().equals(TuningClusterMetric.PURITY)
			|| getTuningClusterMetric().equals(TuningClusterMetric.NMI)
			|| getTuningClusterMetric().equals(TuningClusterMetric.RI)
			|| getTuningClusterMetric().equals(TuningClusterMetric.ARI);
	}

	@Override
	ParamInfo <Double> getMetricParamInfo() {
		return getTuningClusterMetric().getMetricKey();
	}
}
