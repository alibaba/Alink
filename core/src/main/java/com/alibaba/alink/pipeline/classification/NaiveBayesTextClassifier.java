package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTextTrainBatchOp;
import com.alibaba.alink.params.classification.NaiveBayesTextPredictParams;
import com.alibaba.alink.params.classification.NaiveBayesTextTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Text Naive Bayes Classifier.
 * <p>
 * We support the multinomial Naive Bayes and multinomial Naive Bayes model, a probabilistic learning method.
 * Here, feature values of train table must be nonnegative.
 * <p>
 * Details info of the algorithm:
 * https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html
 */
public class NaiveBayesTextClassifier
	extends Trainer <NaiveBayesTextClassifier, NaiveBayesTextModel> implements
	NaiveBayesTextTrainParams <NaiveBayesTextClassifier>,
	NaiveBayesTextPredictParams <NaiveBayesTextClassifier>,
	HasLazyPrintModelInfo <NaiveBayesTextClassifier> {

	private static final long serialVersionUID = 8716690390450232567L;

	public NaiveBayesTextClassifier() {
		super();
	}

	public NaiveBayesTextClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new NaiveBayesTextTrainBatchOp(this.getParams()).linkFrom(in);
	}

}