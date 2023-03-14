package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.classification.NaiveBayesPredictParams;
import com.alibaba.alink.params.classification.NaiveBayesTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Naive Bayes classifier is a simple probability classification algorithm using
 * Bayes theorem based on independent assumption. It is an independent feature model.
 * The input feature can be continual or categorical.
 */
@NameCn("朴素贝叶斯")
public class NaiveBayes extends Trainer <NaiveBayes, NaiveBayesModel>
	implements NaiveBayesTrainParams <NaiveBayes>,
	NaiveBayesPredictParams <NaiveBayes>, HasLazyPrintModelInfo <NaiveBayes> {

	private static final long serialVersionUID = 154387032850937460L;

	public NaiveBayes() {
		super();
	}

	public NaiveBayes(Params params) {
		super(params);
	}

}
