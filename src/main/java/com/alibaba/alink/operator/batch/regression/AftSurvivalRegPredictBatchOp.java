package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.regression.AFTModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.regression.AftRegPredictParams;

/**
 * Accelerated Failure Time Survival Regression.
 * Based on the Weibull distribution of the survival time.
 * <p>
 * (https://en.wikipedia.org/wiki/Accelerated_failure_time_model)
 */
public class AftSurvivalRegPredictBatchOp extends ModelMapBatchOp <AftSurvivalRegPredictBatchOp>
	implements AftRegPredictParams <AftSurvivalRegPredictBatchOp> {

	/**
	 * Constructor.
	 */
	public AftSurvivalRegPredictBatchOp() {
		this(new Params());
	}

	/**
	 * Constructor with the algorithm params.
	 */
	public AftSurvivalRegPredictBatchOp(Params params) {
		super(AFTModelMapper::new, params);
	}
}
