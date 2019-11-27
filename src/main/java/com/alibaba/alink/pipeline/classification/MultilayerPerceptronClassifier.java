package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.MultilayerPerceptronTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.MultilayerPerceptronPredictParams;
import com.alibaba.alink.params.classification.MultilayerPerceptronTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * MultilayerPerceptronClassifier is a neural network based multi-class classifier.
 * Valina neural network with all dense layers are used, the output layer is a softmax layer.
 * Number of inputs has to be equal to the size of feature vectors.
 * Number of outputs has to be equal to the total number of labels.
 */
public class MultilayerPerceptronClassifier
	extends Trainer <MultilayerPerceptronClassifier, MultilayerPerceptronClassificationModel> implements
	MultilayerPerceptronTrainParams <MultilayerPerceptronClassifier>,
	MultilayerPerceptronPredictParams <MultilayerPerceptronClassifier> {

	public MultilayerPerceptronClassifier() {
		this(new Params());
	}

	public MultilayerPerceptronClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new MultilayerPerceptronTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
