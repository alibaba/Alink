package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * String nearest neighbor pipeline model.
 */
public class StringNearestNeighborModel extends MapModel <StringNearestNeighborModel>
	implements NearestNeighborPredictParams <StringNearestNeighborModel> {

	private static final long serialVersionUID = -5499350134609011893L;

	public StringNearestNeighborModel() {
		this(null);
	}

	public StringNearestNeighborModel(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
