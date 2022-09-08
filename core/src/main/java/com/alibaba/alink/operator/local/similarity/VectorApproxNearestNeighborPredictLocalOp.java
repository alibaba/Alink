package com.alibaba.alink.operator.local.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

/**
 * Find the approximate nearest neighbor of query vectors.
 */
@NameCn("向量近似最近邻预测")
public class VectorApproxNearestNeighborPredictLocalOp
	extends ModelMapLocalOp <VectorApproxNearestNeighborPredictLocalOp>
	implements NearestNeighborPredictParams <VectorApproxNearestNeighborPredictLocalOp> {

	public VectorApproxNearestNeighborPredictLocalOp() {
		this(new Params());
	}

	public VectorApproxNearestNeighborPredictLocalOp(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
