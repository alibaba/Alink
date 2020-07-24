package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelMapper;
import com.alibaba.alink.params.classification.NaiveBayesTextPredictParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Text Naive Bayes Predictor.
 *
 * We support the multinomial Naive Bayes and bernoulli Naive Bayes model, a probabilistic learning method.
 * Here, the input data must be vector and the values must be nonnegative.
 *
 * Details info of the algorithm:
 * https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html
 *
 */
public final class NaiveBayesTextPredictBatchOp extends ModelMapBatchOp<NaiveBayesTextPredictBatchOp>
	implements NaiveBayesTextPredictParams<NaiveBayesTextPredictBatchOp> {

	public NaiveBayesTextPredictBatchOp() {
		this(null);
	}

	/**
	 * constructor.
	 *
	 * @param params the parameters set.
	 */
	public NaiveBayesTextPredictBatchOp(Params params) {
		super(NaiveBayesTextModelMapper::new, params);
	}
}
