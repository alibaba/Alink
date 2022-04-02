package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * String approximate nearest neighbor pipeline model.
 */
@NameCn("字符串近似最近邻模型")
public class StringApproxNearestNeighborModel extends MapModel <StringApproxNearestNeighborModel>
	implements NearestNeighborPredictParams <StringApproxNearestNeighborModel> {

	private static final long serialVersionUID = 3211910857263954316L;

	public StringApproxNearestNeighborModel() {
		this(null);
	}

	public StringApproxNearestNeighborModel(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
