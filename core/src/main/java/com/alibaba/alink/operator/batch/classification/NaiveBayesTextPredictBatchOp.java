package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelMapper;
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
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("朴素贝叶斯文本分类预测")
@NameEn("Naive Bayes Text Prediction")
public final class NaiveBayesTextPredictBatchOp extends ModelMapBatchOp <NaiveBayesTextPredictBatchOp>
	implements NaiveBayesTextPredictParams <NaiveBayesTextPredictBatchOp> {

	private static final long serialVersionUID = -5330263374366446258L;

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
