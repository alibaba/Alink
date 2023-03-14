package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;
import com.alibaba.alink.params.similarity.VectorNearestNeighborTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Find the nearest neighbor of query vectors.
 */
@NameCn("向量最近邻")
public class VectorNearestNeighbor extends Trainer <VectorNearestNeighbor, VectorNearestNeighborModel>
	implements VectorNearestNeighborTrainParams<VectorNearestNeighbor>,
	NearestNeighborPredictParams <VectorNearestNeighbor> {

	private static final long serialVersionUID = 171026473979017010L;

	public VectorNearestNeighbor() {
		this(null);
	}

	public VectorNearestNeighbor(Params params) {
		super(params);
	}

}
