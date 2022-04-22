package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelMapper;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.NaiveBayesPredictParams;

/**
 * Naive Bayes Predictor.
 */
@NameCn("朴素贝叶斯预测")
public class NaiveBayesPredictStreamOp extends ModelMapStreamOp <NaiveBayesPredictStreamOp>
	implements NaiveBayesPredictParams <NaiveBayesPredictStreamOp> {

	private static final long serialVersionUID = 4270879350303750921L;

	public NaiveBayesPredictStreamOp() {
		super(NaiveBayesModelMapper::new, new Params());
	}

	public NaiveBayesPredictStreamOp(Params params) {
		super(NaiveBayesModelMapper::new, params);
	}

	public NaiveBayesPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public NaiveBayesPredictStreamOp(BatchOperator model, Params params) {
		super(model, NaiveBayesModelMapper::new, params);
	}
}
