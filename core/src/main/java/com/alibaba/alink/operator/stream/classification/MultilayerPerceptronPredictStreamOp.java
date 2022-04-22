package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.ann.MlpcModelMapper;
import com.alibaba.alink.operator.common.feature.MultiHotModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.MultilayerPerceptronPredictParams;

/**
 * Make stream prediction based on the multilayer perceptron model fitted by MultilayerPerceptronTrainBatchOp.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("多层感知机分类预测")
public final class MultilayerPerceptronPredictStreamOp
	extends ModelMapStreamOp <MultilayerPerceptronPredictStreamOp>
	implements MultilayerPerceptronPredictParams <MultilayerPerceptronPredictStreamOp> {

	private static final long serialVersionUID = 8204591230800526497L;

	public MultilayerPerceptronPredictStreamOp() {
		super(MlpcModelMapper::new, new Params());
	}

	public MultilayerPerceptronPredictStreamOp(Params params) {
		super(MlpcModelMapper::new, params);
	}

	public MultilayerPerceptronPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public MultilayerPerceptronPredictStreamOp(BatchOperator model, Params params) {
		super(model, MlpcModelMapper::new, params);
	}
}
