package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTextTrainBatchOp;
import com.alibaba.alink.params.classification.NaiveBayesTextPredictParams;
import com.alibaba.alink.params.classification.NaiveBayesTextTrainParams;
import com.alibaba.alink.pipeline.Trainer;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Text Naive Bayes Classifier.
 *
 * We support the multinomial Naive Bayes and multinomial Naive Bayes model, a probabilistic learning method.
 * Here, feature values of train table must be nonnegative.
 *
 * Details info of the algorithm:
 * https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html
 */
public class NaiveBayesTextClassifier
	extends Trainer<NaiveBayesTextClassifier, NaiveBayesTextModel> implements
		NaiveBayesTextTrainParams<NaiveBayesTextClassifier>,
        NaiveBayesTextPredictParams<NaiveBayesTextClassifier> {

	public NaiveBayesTextClassifier() {
		super();
	}

	public NaiveBayesTextClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new NaiveBayesTextTrainBatchOp(this.getParams()).linkFrom(in);
	}

}