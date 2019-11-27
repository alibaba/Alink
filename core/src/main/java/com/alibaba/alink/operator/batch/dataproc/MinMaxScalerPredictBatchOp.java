package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.MinMaxScalerModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.dataproc.MinMaxScalerPredictParams;

/**
 * MinMaxScaler transforms a dataSet of rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerPredict will scale the dataSet with model which trained from MaxAbsTrain.
 */
public final class MinMaxScalerPredictBatchOp extends ModelMapBatchOp <MinMaxScalerPredictBatchOp>
	implements MinMaxScalerPredictParams <MinMaxScalerPredictBatchOp> {

	public MinMaxScalerPredictBatchOp() {
		this(new Params());
	}

	public MinMaxScalerPredictBatchOp(Params params) {
		super(MinMaxScalerModelMapper::new, params);
	}

}
