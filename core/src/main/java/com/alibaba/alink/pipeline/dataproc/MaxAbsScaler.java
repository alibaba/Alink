package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MaxAbsScalerTrainBatchOp;
import com.alibaba.alink.params.dataproc.MaxAbsScalerPredictParams;
import com.alibaba.alink.params.dataproc.MaxAbsScalerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * MaxAbsScaler transforms a dataset of Vector rows,rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 */
public class MaxAbsScaler extends Trainer <MaxAbsScaler, MaxAbsScalerModel> implements
	MaxAbsScalerTrainParams <MaxAbsScaler>,
	MaxAbsScalerPredictParams <MaxAbsScaler> {

	public MaxAbsScaler() {
		super();
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new MaxAbsScalerTrainBatchOp(this.getParams()).linkFrom(in);
	}
}

