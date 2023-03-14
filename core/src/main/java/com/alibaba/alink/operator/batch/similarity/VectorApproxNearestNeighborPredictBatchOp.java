package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

/**
 * Find the approximate nearest neighbor of query vectors.
 */
@NameCn("向量近似最近邻预测")
@NameEn("Vector Approx Nearest Neighbor Prediction")
public class VectorApproxNearestNeighborPredictBatchOp
	extends ModelMapBatchOp <VectorApproxNearestNeighborPredictBatchOp>
	implements NearestNeighborPredictParams <VectorApproxNearestNeighborPredictBatchOp> {

	private static final long serialVersionUID = -3762462541853312430L;

	public VectorApproxNearestNeighborPredictBatchOp() {
		this(new Params());
	}

	public VectorApproxNearestNeighborPredictBatchOp(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
