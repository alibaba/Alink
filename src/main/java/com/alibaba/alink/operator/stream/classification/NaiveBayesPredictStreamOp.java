package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.classification.NaiveBayesModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.NaiveBayesPredictParams;

/**
 * Naive Bayes Predictor.
 *
 * We support the multinomial Naive Bayes and multinomial NB model, a probabilistic learning method.
 * here, feature values of train table must be nonnegative.
 *
 * Details info of the algorithm:
 * https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html
 *
 */
public final class NaiveBayesPredictStreamOp extends ModelMapStreamOp <NaiveBayesPredictStreamOp>
	implements NaiveBayesPredictParams <NaiveBayesPredictStreamOp> {

	public NaiveBayesPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public NaiveBayesPredictStreamOp(BatchOperator model, Params params) {
		super(model, NaiveBayesModelMapper::new, params);
	}
}
