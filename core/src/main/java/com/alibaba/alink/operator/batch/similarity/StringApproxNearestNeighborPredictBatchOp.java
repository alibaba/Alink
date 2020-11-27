package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

/**
 * Find the approximate nearest neighbor of query string.
 */
public class StringApproxNearestNeighborPredictBatchOp
	extends ModelMapBatchOp <StringApproxNearestNeighborPredictBatchOp>
	implements NearestNeighborPredictParams <StringApproxNearestNeighborPredictBatchOp> {

	private static final long serialVersionUID = 3715339958211990196L;

	public StringApproxNearestNeighborPredictBatchOp() {
		this(new Params());
	}

	public StringApproxNearestNeighborPredictBatchOp(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
