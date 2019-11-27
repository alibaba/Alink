package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;

/**
 * The batch operator that predict the data using the quantile discretizer model.
 */
public final class QuantileDiscretizerPredictBatchOp extends ModelMapBatchOp <QuantileDiscretizerPredictBatchOp>
	implements QuantileDiscretizerPredictParams <QuantileDiscretizerPredictBatchOp> {

	public QuantileDiscretizerPredictBatchOp() {
		this(null);
	}

	public QuantileDiscretizerPredictBatchOp(Params params) {
		super(QuantileDiscretizerModelMapper::new, params);
	}
}
