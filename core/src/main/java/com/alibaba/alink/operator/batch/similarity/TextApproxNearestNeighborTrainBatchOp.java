package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.similarity.TrainType;
import com.alibaba.alink.operator.common.similarity.dataConverter.StringModelDataConverter;
import com.alibaba.alink.params.similarity.StringTextApproxNearestNeighborTrainParams;

/**
 * Find the approximate nearest neighbor of query texts.
 */
public class TextApproxNearestNeighborTrainBatchOp
	extends BaseNearestNeighborTrainBatchOp<TextApproxNearestNeighborTrainBatchOp>
	implements StringTextApproxNearestNeighborTrainParams <TextApproxNearestNeighborTrainBatchOp> {

	private static final long serialVersionUID = 7849910987059476012L;

	public TextApproxNearestNeighborTrainBatchOp() {
		this(new Params());
	}

	public TextApproxNearestNeighborTrainBatchOp(Params params) {
		super(params.set(StringModelDataConverter.TEXT, true)
			.set(BaseNearestNeighborTrainBatchOp.TRAIN_TYPE, TrainType.APPROX_TEXT));
	}
}
