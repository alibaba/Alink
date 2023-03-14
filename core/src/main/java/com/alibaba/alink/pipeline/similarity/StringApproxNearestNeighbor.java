package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;
import com.alibaba.alink.params.similarity.StringTextApproxNearestNeighborTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Find the approximate nearest neighbor of query string.
 */
@NameCn("字符串近似最近邻")
public class StringApproxNearestNeighbor
	extends Trainer <StringApproxNearestNeighbor, StringApproxNearestNeighborModel>
	implements StringTextApproxNearestNeighborTrainParams <StringApproxNearestNeighbor>,
	NearestNeighborPredictParams <StringApproxNearestNeighbor> {

	private static final long serialVersionUID = -1402702486415693096L;

	public StringApproxNearestNeighbor() {
		this(null);
	}

	public StringApproxNearestNeighbor(Params params) {
		super(params);
	}

}
