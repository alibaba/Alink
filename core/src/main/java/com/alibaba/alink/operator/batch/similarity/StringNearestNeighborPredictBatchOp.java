package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

/**
 * Find the nearest neighbor of query string.
 */
public class StringNearestNeighborPredictBatchOp extends ModelMapBatchOp <StringNearestNeighborPredictBatchOp>
	implements NearestNeighborPredictParams <StringNearestNeighborPredictBatchOp> {

	private static final long serialVersionUID = -5024770436670979509L;

	public StringNearestNeighborPredictBatchOp() {
		this(new Params());
	}

	public StringNearestNeighborPredictBatchOp(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
