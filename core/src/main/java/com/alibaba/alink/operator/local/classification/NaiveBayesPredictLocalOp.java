package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.classification.NaiveBayesPredictParams;

/**
 * Naive Bayes Predictor.
 */
@NameCn("朴素贝叶斯预测")
public class NaiveBayesPredictLocalOp extends ModelMapLocalOp <NaiveBayesPredictLocalOp>
	implements NaiveBayesPredictParams <NaiveBayesPredictLocalOp> {

	public NaiveBayesPredictLocalOp() {
		this(null);
	}

	public NaiveBayesPredictLocalOp(Params params) {
		super(NaiveBayesModelMapper::new, params);
	}
}
