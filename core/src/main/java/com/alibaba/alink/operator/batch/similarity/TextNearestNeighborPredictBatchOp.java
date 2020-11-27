package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

/**
 * Find the nearest neighbor of query texts.
 */
public class TextNearestNeighborPredictBatchOp extends ModelMapBatchOp <TextNearestNeighborPredictBatchOp>
	implements NearestNeighborPredictParams <TextNearestNeighborPredictBatchOp> {

	private static final long serialVersionUID = -464374820231352727L;

	public TextNearestNeighborPredictBatchOp() {
		this(new Params());
	}

	public TextNearestNeighborPredictBatchOp(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
