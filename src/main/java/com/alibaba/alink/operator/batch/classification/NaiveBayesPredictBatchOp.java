package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.classification.NaiveBayesModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.classification.NaiveBayesPredictParams;

/**
 * Naive Bayes Predictor.
 *
 * We support the multinomial Naive Bayes and multinomial NB model, a probabilistic learning method.
 * here, feature values of train table must be nonnegative.
 */
public final class NaiveBayesPredictBatchOp extends ModelMapBatchOp <NaiveBayesPredictBatchOp>
	implements NaiveBayesPredictParams <NaiveBayesPredictBatchOp> {

	public NaiveBayesPredictBatchOp() {
		this(null);
	}

	/**
	 * constructor.
	 *
	 * @param params the parameters set.
	 */
	public NaiveBayesPredictBatchOp(Params params) {
		super(NaiveBayesModelMapper::new, params);
	}
}
