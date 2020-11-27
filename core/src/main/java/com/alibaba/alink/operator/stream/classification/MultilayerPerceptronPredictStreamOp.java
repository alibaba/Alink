package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.ann.MlpcModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.MultilayerPerceptronPredictParams;

/**
 * Make stream prediction based on the multilayer perceptron model fitted by MultilayerPerceptronTrainBatchOp.
 */
public final class MultilayerPerceptronPredictStreamOp
	extends ModelMapStreamOp <MultilayerPerceptronPredictStreamOp>
	implements MultilayerPerceptronPredictParams <MultilayerPerceptronPredictStreamOp> {

	private static final long serialVersionUID = 8204591230800526497L;

	public MultilayerPerceptronPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public MultilayerPerceptronPredictStreamOp(BatchOperator model, Params params) {
		super(model, MlpcModelMapper::new, params);
	}
}
