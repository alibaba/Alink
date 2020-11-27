package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.classification.KnnMapper;
import com.alibaba.alink.params.classification.KnnPredictParams;

/**
 * KNN batch predictor.
 */
public final class KnnPredictBatchOp extends ModelMapBatchOp <KnnPredictBatchOp>
	implements KnnPredictParams <KnnPredictBatchOp> {

	private static final long serialVersionUID = -3118065094037473283L;

	public KnnPredictBatchOp() {
		this(null);
	}

	public KnnPredictBatchOp(Params params) {
		super(KnnMapper::new, params);
	}
}
