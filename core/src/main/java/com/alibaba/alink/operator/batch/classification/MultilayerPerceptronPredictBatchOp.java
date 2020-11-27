package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.classification.ann.MlpcModelMapper;
import com.alibaba.alink.params.classification.MultilayerPerceptronPredictParams;

/**
 * Make prediction based on the multilayer perceptron model fitted by MultilayerPerceptronTrainBatchOp.
 */
public class MultilayerPerceptronPredictBatchOp
	extends ModelMapBatchOp <MultilayerPerceptronPredictBatchOp>
	implements MultilayerPerceptronPredictParams <MultilayerPerceptronPredictBatchOp> {

	private static final long serialVersionUID = 6283020784990848132L;

	public MultilayerPerceptronPredictBatchOp() {
		this(new Params());
	}

	public MultilayerPerceptronPredictBatchOp(Params params) {
		super(MlpcModelMapper::new, params);
	}
}
