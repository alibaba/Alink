package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.NaiveBayesPredictParams;
import com.alibaba.alink.params.classification.NaiveBayesTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Naive Bayes Classifier.
 *
 * We support the multinomial Naive Bayes and multinomial Naive Bayes model, a probabilistic learning method.
 * Here, feature values of train table must be nonnegative.
 *
 * Details info of the algorithm:
 * https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html
 */
public class NaiveBayes
	extends Trainer <NaiveBayes, NaiveBayesModel> implements
	NaiveBayesTrainParams <NaiveBayes>,
	NaiveBayesPredictParams <NaiveBayes> {

	public NaiveBayes() {
		super();
	}

	public NaiveBayes(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new NaiveBayesTrainBatchOp(this.getParams()).linkFrom(in);
	}

}