package com.alibaba.alink.pipeline.dataproc.vector;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorStandardScalerTrainBatchOp;
import com.alibaba.alink.params.dataproc.vector.VectorStandardPredictParams;
import com.alibaba.alink.params.dataproc.vector.VectorStandardTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * The transformer standard the value of the vector using the following formula:
 *
 * x_scaled = (x - mean)／sigma, where mean is the mean value of column, sigma is the standard variance.
 */
public class VectorStandardScaler extends Trainer <VectorStandardScaler, VectorStandardScalerModel> implements
	VectorStandardTrainParams <VectorStandardScaler>,
	VectorStandardPredictParams <VectorStandardScaler> {

	public VectorStandardScaler() {
		super();
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new VectorStandardScalerTrainBatchOp(this.getParams()).linkFrom(in);
	}
}

