package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.classification.MultilayerPerceptronPredictParams;
import com.alibaba.alink.params.classification.MultilayerPerceptronTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * MultilayerPerceptronClassifier is a neural network based multi-class classifier.
 * Valina neural network with all dense layers are used, the output layer is a softmax layer.
 * Number of inputs has to be equal to the size of feature vectors.
 * Number of outputs has to be equal to the total number of labels.
 */
@NameCn("多层感知机分类")
public class MultilayerPerceptronClassifier
	extends Trainer <MultilayerPerceptronClassifier, MultilayerPerceptronClassificationModel> implements
	MultilayerPerceptronTrainParams <MultilayerPerceptronClassifier>,
	MultilayerPerceptronPredictParams <MultilayerPerceptronClassifier> {

	private static final long serialVersionUID = 4347540859245296560L;

	public MultilayerPerceptronClassifier() {
		this(new Params());
	}

	public MultilayerPerceptronClassifier(Params params) {
		super(params);
	}

}
