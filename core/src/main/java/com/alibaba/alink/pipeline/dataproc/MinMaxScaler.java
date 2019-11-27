package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MinMaxScalerTrainBatchOp;
import com.alibaba.alink.params.dataproc.MinMaxScalerPredictParams;
import com.alibaba.alink.params.dataproc.MinMaxScalerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * MinMaxScaler transforms a dataset of Vector rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 */
public class MinMaxScaler extends Trainer <MinMaxScaler, MinMaxScalerModel> implements
	MinMaxScalerTrainParams <MinMaxScaler>,
	MinMaxScalerPredictParams <MinMaxScaler> {

	public MinMaxScaler() {
		super();
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new MinMaxScalerTrainBatchOp(this.getParams()).linkFrom(in);
	}
}

