package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.GlmEvaluationBatchOp;
import com.alibaba.alink.operator.common.regression.GlmModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.GlmPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Generalized Linear Model.
 */
public class GeneralizedLinearRegressionModel extends MapModel<GeneralizedLinearRegressionModel>
	implements GlmPredictParams <GeneralizedLinearRegressionModel> {

	public GeneralizedLinearRegressionModel() {
		this(null);
	}

	public GeneralizedLinearRegressionModel(Params params) {
		super(GlmModelMapper::new, params);
	}

	public BatchOperator evaluate(BatchOperator in) {
		return new GlmEvaluationBatchOp(this.getParams()).linkFrom(
			BatchOperator.fromTable(this.getModelData()).setMLEnvironmentId(getMLEnvironmentId()), in);
	}
}