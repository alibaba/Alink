package com.alibaba.alink.pipeline.dataproc.vector;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorMinMaxScalerTrainBatchOp;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerPredictParams;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * The transformer normalizes the value of the vector to [0,1] using the following formula:
 *
 * x_scaled = (x - eMin) / (eMax - eMin) * (maxV - minV) + minV;
 */
public class VectorMinMaxScaler extends Trainer <VectorMinMaxScaler, VectorMinMaxScalerModel> implements
	VectorMinMaxScalerTrainParams <VectorMinMaxScaler>,
	VectorMinMaxScalerPredictParams <VectorMinMaxScaler> {

	public VectorMinMaxScaler() {
		super();
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new VectorMinMaxScalerTrainBatchOp(this.getParams()).linkFrom(in);
	}
}

