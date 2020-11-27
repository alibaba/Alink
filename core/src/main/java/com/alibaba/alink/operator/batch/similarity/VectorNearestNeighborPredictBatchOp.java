package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

/**
 * Find the nearest neighbor of query vectors.
 */
public class VectorNearestNeighborPredictBatchOp extends ModelMapBatchOp <VectorNearestNeighborPredictBatchOp>
	implements NearestNeighborPredictParams <VectorNearestNeighborPredictBatchOp> {

	private static final long serialVersionUID = 6555529390162333788L;

	public VectorNearestNeighborPredictBatchOp() {
		this(new Params());
	}

	public VectorNearestNeighborPredictBatchOp(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
