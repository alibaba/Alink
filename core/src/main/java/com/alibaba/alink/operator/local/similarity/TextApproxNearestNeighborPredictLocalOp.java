package com.alibaba.alink.operator.local.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

/**
 * Find the approximate nearest neighbor of query texts.
 */
@NameCn("文本近似最近邻预测")
public class TextApproxNearestNeighborPredictLocalOp extends ModelMapLocalOp <TextApproxNearestNeighborPredictLocalOp>
	implements NearestNeighborPredictParams <TextApproxNearestNeighborPredictLocalOp> {

	public TextApproxNearestNeighborPredictLocalOp() {
		this(new Params());
	}

	public TextApproxNearestNeighborPredictLocalOp(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
