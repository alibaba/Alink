package com.alibaba.alink.pipeline.dataproc.vector;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorMaxAbsScalerTrainBatchOp;
import com.alibaba.alink.params.dataproc.vector.VectorMaxAbsScalerPredictParams;
import com.alibaba.alink.params.dataproc.vector.VectorMaxAbsScalerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * The transformer normalizes the value of the vector to [-1,1] using the following formula:
 *
 * x_scaled = x / abs(X_max)
 */
public class VectorMaxAbsScaler extends Trainer <VectorMaxAbsScaler, VectorMaxAbsScalerModel> implements
	VectorMaxAbsScalerTrainParams <VectorMaxAbsScaler>,
	VectorMaxAbsScalerPredictParams <VectorMaxAbsScaler> {

	public VectorMaxAbsScaler() {
		super();
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new VectorMaxAbsScalerTrainBatchOp(this.getParams()).linkFrom(in);
	}
}

