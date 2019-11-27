package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.classification.LogisticRegressionPredictParams;

/**
 * Logistic regression predict batch operator. this operator predict data's label with linear model.
 *
 */
public final class LogisticRegressionPredictBatchOp extends ModelMapBatchOp <LogisticRegressionPredictBatchOp>
	implements LogisticRegressionPredictParams <LogisticRegressionPredictBatchOp> {

	public LogisticRegressionPredictBatchOp() {
		this(new Params());
	}

	public LogisticRegressionPredictBatchOp(Params params) {
		super(LinearModelMapper::new, params);
	}

}
