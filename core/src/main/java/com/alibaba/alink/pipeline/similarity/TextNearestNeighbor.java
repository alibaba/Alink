package com.alibaba.alink.pipeline.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.similarity.TextNearestNeighborTrainBatchOp;
import com.alibaba.alink.params.similarity.NearestNeighborPredictParams;
import com.alibaba.alink.params.similarity.StringTextNearestNeighborTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Find the nearest neighbor of query texts.
 */
@NameCn("文本最近邻")
public class TextNearestNeighbor extends Trainer <TextNearestNeighbor, TextNearestNeighborModel>
	implements StringTextNearestNeighborTrainParams<TextNearestNeighbor>,
	NearestNeighborPredictParams <TextNearestNeighbor> {

	private static final long serialVersionUID = 1268955460048381622L;

	public TextNearestNeighbor() {
		this(null);
	}

	public TextNearestNeighbor(Params params) {
		super(params);
	}

	@Override
	public BatchOperator <?> train(BatchOperator <?> in) {
		return new TextNearestNeighborTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
