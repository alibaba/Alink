package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.KnnTrainBatchOp;
import com.alibaba.alink.params.classification.KnnPredictParams;
import com.alibaba.alink.params.classification.KnnTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * KNN classifier is to classify unlabeled observations by assigning them to the class of the most similar
 * labeled examples.
 */
public class KnnClassifier extends Trainer <KnnClassifier, KnnClassificationModel> implements
	KnnTrainParams <KnnClassifier>, KnnPredictParams <KnnClassifier> {

	private static final long serialVersionUID = 5292477422193301398L;

	public KnnClassifier() {
		super();
	}

	public KnnClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new KnnTrainBatchOp(params).linkFrom(in);
	}

}
