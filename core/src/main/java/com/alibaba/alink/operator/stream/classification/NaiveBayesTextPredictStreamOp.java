package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.NaiveBayesTextPredictParams;

/**
 * Text Naive Bayes Predictor.
 *
 * We support the multinomial Naive Bayes and bernoulli Naive Bayes model, a probabilistic learning method.
 * Here, the input data must be vector and the values must be nonnegative.
 *
 * Details info of the algorithm:
 * https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html
 */
public final class NaiveBayesTextPredictStreamOp extends ModelMapStreamOp <NaiveBayesTextPredictStreamOp>
	implements NaiveBayesTextPredictParams <NaiveBayesTextPredictStreamOp> {

	private static final long serialVersionUID = -2382102179447174346L;

	public NaiveBayesTextPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public NaiveBayesTextPredictStreamOp(BatchOperator model, Params params) {
		super(model, NaiveBayesTextModelMapper::new, params);
	}
}
