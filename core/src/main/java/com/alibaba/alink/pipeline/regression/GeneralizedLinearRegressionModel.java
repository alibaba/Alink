package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.GlmEvaluationBatchOp;
import com.alibaba.alink.operator.common.regression.GlmModelMapper;
import com.alibaba.alink.params.regression.GlmPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Generalized Linear Model.
 */
public class GeneralizedLinearRegressionModel extends MapModel <GeneralizedLinearRegressionModel>
	implements GlmPredictParams <GeneralizedLinearRegressionModel> {

	private static final long serialVersionUID = -7701571839951233571L;

	public GeneralizedLinearRegressionModel() {
		this(null);
	}

	public GeneralizedLinearRegressionModel(Params params) {
		super(GlmModelMapper::new, params);
	}

	public BatchOperator <?> evaluate(BatchOperator <?> in) {
		return new GlmEvaluationBatchOp(this.getParams()).linkFrom(this.getModelData(), in);
	}

}