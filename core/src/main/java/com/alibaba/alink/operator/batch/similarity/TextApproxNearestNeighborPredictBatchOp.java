package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;

/**
 * Find the approximate nearest neighbor of query texts.
 */
@NameCn("文本近似最近邻预测")
@NameEn("Text Approx Nearest Neighbor Prediction")
public class TextApproxNearestNeighborPredictBatchOp extends ModelMapBatchOp <TextApproxNearestNeighborPredictBatchOp>
	implements NearestNeighborPredictParams <TextApproxNearestNeighborPredictBatchOp> {

	private static final long serialVersionUID = -5810550818671846741L;

	public TextApproxNearestNeighborPredictBatchOp() {
		this(new Params());
	}

	public TextApproxNearestNeighborPredictBatchOp(Params params) {
		super(NearestNeighborsMapper::new, params);
	}
}
