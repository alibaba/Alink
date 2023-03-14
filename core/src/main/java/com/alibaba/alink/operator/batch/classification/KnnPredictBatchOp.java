package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.classification.KnnMapper;
import com.alibaba.alink.params.classification.KnnPredictParams;

/**
 * KNN batch predictor.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("最近邻分类预测")
@NameEn("Knn Prediction")
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
