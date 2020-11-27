package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.nlp.HasVocabSize;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.params.shared.iter.HasNumIterDefaultAs10;

/**
 * Parameter of LDA train.
 */
public interface LdaTrainParams<T> extends
	HasNumIterDefaultAs10 <T>,
	HasVocabSize <T>,
	HasSelectedCol <T>,
	HasRandomSeed <T> {
	ParamInfo <Integer> TOPIC_NUM = ParamInfoFactory
		.createParamInfo("topicNum", Integer.class)
		.setDescription("Number of topic.")
		.setRequired()
		.build();
	ParamInfo <Double> ALPHA = ParamInfoFactory
		.createParamInfo("alpha", Double.class)
		.setDescription(
			"alpha.Concentration parameter (commonly named \"alpha\") for the prior placed on documents' distributions"
				+ " over topics (\"beta\").")
		.setHasDefaultValue(-1.0)
		.build();
	ParamInfo <Double> BETA = ParamInfoFactory
		.createParamInfo("beta", Double.class)
		.setDescription(
			"Concentration parameter (commonly named \"beta\" or \"eta\") for the prior placed on topics' "
				+ "distributions over terms.")
		.setHasDefaultValue(-1.0)
		.build();
	ParamInfo <Method> METHOD = ParamInfoFactory
		.createParamInfo("method", Method.class)
		.setDescription("optimizer: em, online")
		.setHasDefaultValue(Method.EM)
		.setAlias(new String[] {"optimizer"})
		.build();

	enum Method {
		Online,
		EM
	}

	ParamInfo <Double> ONLINE_LEARNING_OFFSET = ParamInfoFactory
		.createParamInfo("onlineLearningOffset", Double.class)
		.setDescription("(For online optimizer)" +
			" A (positive) learning parameter that downweights early iterations. Larger values make early" +
			" iterations count less.")
		.setHasDefaultValue(1024.0)
		.build();
	ParamInfo <Double> LEARNING_DECAY = ParamInfoFactory
		.createParamInfo("learningDecay", Double.class)
		.setDescription("(For online optimizer) " +
			" Learning rate, set as an exponential decay rate. This should be between (0.5, 1.0] to" +
			" guarantee asymptotic convergence.")
		.setHasDefaultValue(0.51)
		.build();
	ParamInfo <Double> SUBSAMPLING_RATE = ParamInfoFactory
		.createParamInfo("subsamplingRate", Double.class)
		.setDescription("For online optimizer " +
			"Fraction of the corpus to be sampled and used in each iteration of mini-batch" +
			"gradient descent, in range (0, 1].")
		.setHasDefaultValue(0.05)
		.build();
	ParamInfo <Boolean> OPTIMIZE_DOC_CONCENTRATION = ParamInfoFactory
		.createParamInfo("optimizeDocConcentration", Boolean.class)
		.setDescription("(For online optimizer only, currently) Indicates whether the docConcentration" +
			"(Dirichlet parameter for document-topic distribution) will be optimized during training.")
		.setHasDefaultValue(true)
		.build();

	default Integer getTopicNum() {
		return get(TOPIC_NUM);
	}

	default T setTopicNum(Integer value) {
		return set(TOPIC_NUM, value);
	}

	default Double getAlpha() {
		return get(ALPHA);
	}

	default T setAlpha(Double value) {
		return set(ALPHA, value);
	}

	default Double getBeta() {
		return get(BETA);
	}

	default T setBeta(Double value) {
		return set(BETA, value);
	}

	default Method getMethod() {
		return get(METHOD);
	}

	default T setMethod(Method value) {
		return set(METHOD, value);
	}

	default T setMethod(String value) {
		return set(METHOD, ParamUtil.searchEnum(METHOD, value));
	}

	default Double getOnlineLearningOffset() {
		return get(ONLINE_LEARNING_OFFSET);
	}

	default T setOnlineLearningOffset(Double value) {
		return set(ONLINE_LEARNING_OFFSET, value);
	}

	default Double getLearningDecay() {
		return get(LEARNING_DECAY);
	}

	default T setLearningDecay(Double value) {
		return set(LEARNING_DECAY, value);
	}

	default Double getSubsamplingRate() {
		return get(SUBSAMPLING_RATE);
	}

	default T setSubsamplingRate(Double value) {
		return set(SUBSAMPLING_RATE, value);
	}

	default Boolean getOptimizeDocConcentration() {
		return get(OPTIMIZE_DOC_CONCENTRATION);
	}

	default T setOptimizeDocConcentration(Boolean value) {
		return set(OPTIMIZE_DOC_CONCENTRATION, value);
	}

}
