package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.classification.NaiveBayesTextPredictParams;

/**
 * Text Naive Bayes Predictor.
 * <p>
 * We support the multinomial Naive Bayes and bernoulli Naive Bayes model, a probabilistic learning method.
 * Here, the input data must be vector and the values must be nonnegative.
 * <p>
 * Details info of the algorithm:
 * https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html
 */
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("朴素贝叶斯文本分类预测")
public final class NaiveBayesTextPredictLocalOp extends ModelMapLocalOp <NaiveBayesTextPredictLocalOp>
	implements NaiveBayesTextPredictParams <NaiveBayesTextPredictLocalOp> {

	public NaiveBayesTextPredictLocalOp() {
		this(null);
	}

	/**
	 * constructor.
	 *
	 * @param params the parameters set.
	 */
	public NaiveBayesTextPredictLocalOp(Params params) {
		super(NaiveBayesTextModelMapper::new, params);
	}
}
