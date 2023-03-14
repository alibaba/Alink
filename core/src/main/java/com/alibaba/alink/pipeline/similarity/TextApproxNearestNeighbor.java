package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;
import com.alibaba.alink.params.similarity.StringTextApproxNearestNeighborTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Find the approximate nearest neighbor of query texts.
 */
@NameCn("文本近似最近邻")
public class TextApproxNearestNeighbor extends Trainer <TextApproxNearestNeighbor, StringApproxNearestNeighborModel>
	implements StringTextApproxNearestNeighborTrainParams <TextApproxNearestNeighbor>,
	NearestNeighborPredictParams <TextApproxNearestNeighbor> {

	private static final long serialVersionUID = 792881206508951591L;

	public TextApproxNearestNeighbor() {
		this(null);
	}

	public TextApproxNearestNeighbor(Params params) {
		super(params);
	}

}
