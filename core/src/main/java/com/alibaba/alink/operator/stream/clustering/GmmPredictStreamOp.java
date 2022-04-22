package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.GmmModelMapper;
import com.alibaba.alink.operator.common.regression.GlmModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.clustering.GmmPredictParams;

/**
 * Gaussian Mixture prediction based on the model fitted by GmmTrainBatchOp.
 */

@ParamSelectColumnSpec(name = "vectorCol", portIndices = 1, allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("高斯混合模型预测")
public final class GmmPredictStreamOp extends ModelMapStreamOp <GmmPredictStreamOp>
	implements GmmPredictParams <GmmPredictStreamOp> {

	private static final long serialVersionUID = -6737997176873713411L;

	public GmmPredictStreamOp() {
		super(GmmModelMapper::new, new Params());
	}

	public GmmPredictStreamOp(Params params) {
		super(GmmModelMapper::new, params);
	}

	public GmmPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public GmmPredictStreamOp(BatchOperator model, Params params) {
		super(model, GmmModelMapper::new, params);
	}
}
