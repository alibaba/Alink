package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelMapper;
import com.alibaba.alink.params.classification.NaiveBayesPredictParams;

/**
 * Naive Bayes Predictor.
 */
@NameCn("朴素贝叶斯预测")
@NameEn("Naive Bayes Prediction")
public class NaiveBayesPredictBatchOp extends ModelMapBatchOp <NaiveBayesPredictBatchOp>
	implements NaiveBayesPredictParams <NaiveBayesPredictBatchOp> {

	private static final long serialVersionUID = 4147658526507997767L;

	public NaiveBayesPredictBatchOp() {
		this(null);
	}

	public NaiveBayesPredictBatchOp(Params params) {
		super(NaiveBayesModelMapper::new, params);
	}
}
