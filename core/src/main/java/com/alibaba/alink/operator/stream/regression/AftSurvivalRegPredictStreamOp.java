package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.regression.AFTModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.AftRegPredictParams;

/**
 * Accelerated Failure Time Survival Regression.
 * Based on the Weibull distribution of the survival time.
 * <p>
 * (https://en.wikipedia.org/wiki/Accelerated_failure_time_model)
 */
public class AftSurvivalRegPredictStreamOp extends ModelMapStreamOp <AftSurvivalRegPredictStreamOp>
	implements AftRegPredictParams <AftSurvivalRegPredictStreamOp> {

	/**
	 * Constructor.
	 */
	public AftSurvivalRegPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	/**
	 * Constructor with the algorithm params.
	 */
	public AftSurvivalRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, AFTModelMapper::new, params);
	}

}
