package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Text approximate nearest neighbor pipeline model.
 */
@NameCn("文本近似最近邻模型")
public class TextApproxNearestNeighborModel extends MapModel <TextApproxNearestNeighborModel>
	implements NearestNeighborPredictParams <TextApproxNearestNeighborModel> {

	private static final long serialVersionUID = 4597689493577291695L;

	public TextApproxNearestNeighborModel() {
		this(null);
	}

	public TextApproxNearestNeighborModel(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
