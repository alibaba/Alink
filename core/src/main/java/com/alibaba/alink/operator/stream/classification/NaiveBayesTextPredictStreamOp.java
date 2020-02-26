package com.alibaba.alink.operator.stream.classification;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.NaiveBayesTextPredictParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Text Naive Bayes Predictor.
 *
 * We support the multinomial Naive Bayes and multinomial NB model, a probabilistic learning method.
 * here, feature values of train table must be nonnegative.
 *
 * Details info of the algorithm:
 * https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html
 *
 */
public final class NaiveBayesTextPredictStreamOp extends ModelMapStreamOp<NaiveBayesTextPredictStreamOp>
	implements NaiveBayesTextPredictParams<NaiveBayesTextPredictStreamOp> {

	public NaiveBayesTextPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public NaiveBayesTextPredictStreamOp(BatchOperator model, Params params) {
		super(model, NaiveBayesTextModelMapper::new, params);
	}
}
