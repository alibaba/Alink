package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Vector approximate nearest neighbor pipeline model.
 */
@NameCn("向量近似最近邻模型")
public class VectorApproxNearestNeighborModel extends MapModel <VectorApproxNearestNeighborModel>
	implements NearestNeighborPredictParams <VectorApproxNearestNeighborModel> {

	private static final long serialVersionUID = -5193744295954406669L;

	public VectorApproxNearestNeighborModel() {
		this(null);
	}

	public VectorApproxNearestNeighborModel(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
