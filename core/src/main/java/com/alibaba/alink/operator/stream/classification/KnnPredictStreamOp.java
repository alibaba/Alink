package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.KnnMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.KnnPredictParams;

/**
 * KNN stream predictor.
 */
@NameCn("KNN流式预测")
public final class KnnPredictStreamOp extends ModelMapStreamOp <KnnPredictStreamOp>
	implements KnnPredictParams <KnnPredictStreamOp> {

	public KnnPredictStreamOp(BatchOperator<?> model) {
		this(model, null);
	}

	public KnnPredictStreamOp(BatchOperator<?> model, Params params) {
		super(model, KnnMapper::new, params);
	}
}
