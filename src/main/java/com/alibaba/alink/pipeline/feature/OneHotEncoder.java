package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.OneHotTrainBatchOp;
import com.alibaba.alink.params.feature.OneHotPredictParams;
import com.alibaba.alink.params.feature.OneHotTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * One hot pipeline op.
 *
 */
public class OneHotEncoder extends Trainer <OneHotEncoder, OneHotEncoderModel> implements
	OneHotTrainParams <OneHotEncoder>,
	OneHotPredictParams <OneHotEncoder> {

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new OneHotTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
