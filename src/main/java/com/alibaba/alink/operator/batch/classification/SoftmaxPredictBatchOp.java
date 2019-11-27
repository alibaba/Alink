package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.linear.SoftmaxModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.classification.SoftmaxPredictParams;

/**
 * Softmax predict batch operator.
 *
 */
public final class SoftmaxPredictBatchOp extends ModelMapBatchOp <SoftmaxPredictBatchOp>
	implements SoftmaxPredictParams <SoftmaxPredictBatchOp> {

	public SoftmaxPredictBatchOp() {
		this(new Params());
	}

	public SoftmaxPredictBatchOp(Params params) {
		super(SoftmaxModelMapper::new, params);
	}

}
