package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.similarity.TrainType;
import com.alibaba.alink.operator.common.similarity.dataConverter.StringModelDataConverter;
import com.alibaba.alink.params.similarity.StringTextNearestNeighborTrainParams;

/**
 * Find the nearest neighbor of query string.
 */
public class StringNearestNeighborTrainBatchOp
	extends BaseNearestNeighborTrainBatchOp<StringNearestNeighborTrainBatchOp>
	implements StringTextNearestNeighborTrainParams<StringNearestNeighborTrainBatchOp> {

	private static final long serialVersionUID = -403468209326834983L;

	public StringNearestNeighborTrainBatchOp() {
		this(new Params());
	}

	public StringNearestNeighborTrainBatchOp(Params params) {
		super(params.set(BaseNearestNeighborTrainBatchOp.TRAIN_TYPE, TrainType.STRING)
			.set(StringModelDataConverter.TEXT, false));
	}
}
