package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.classification.KnnPredictParams;
import com.alibaba.alink.params.classification.KnnTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * KNN classifier is to classify unlabeled observations by assigning them to the class of the most similar
 * labeled examples.
 */
@NameCn("最近邻分类")
public class KnnClassifier extends Trainer <KnnClassifier, KnnClassificationModel> implements
	KnnTrainParams <KnnClassifier>, KnnPredictParams <KnnClassifier> {

	private static final long serialVersionUID = 5292477422193301398L;

	public KnnClassifier() {
		super();
	}

	public KnnClassifier(Params params) {
		super(params);
	}

}
