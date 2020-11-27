package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.similarity.TrainType;
import com.alibaba.alink.operator.common.similarity.dataConverter.StringModelDataConverter;
import com.alibaba.alink.params.similarity.StringTextApproxNearestNeighborTrainParams;

/**
 * Find the approximate nearest neighbor of query string.
 */
public class StringApproxNearestNeighborTrainBatchOp
	extends BaseNearestNeighborTrainBatchOp<StringApproxNearestNeighborTrainBatchOp>
	implements StringTextApproxNearestNeighborTrainParams <StringApproxNearestNeighborTrainBatchOp> {

	private static final long serialVersionUID = -7647838869499679799L;

	public StringApproxNearestNeighborTrainBatchOp() {
		this(new Params());
	}

	public StringApproxNearestNeighborTrainBatchOp(Params params) {
		super(params.set(StringModelDataConverter.TEXT, false)
			.set(BaseNearestNeighborTrainBatchOp.TRAIN_TYPE, TrainType.APPROX_STRING));
	}
}
