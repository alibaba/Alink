package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.ParamUtil;

/**
 * Params for ApproxStringNeareastNeighborIndex.
 */
public interface StringTextNearestNeighborTrainParams<T> extends
	NearestNeighborTrainParams <T>,
	StringTextExactParams <T> {

	ParamInfo <Metric> METRIC = ParamInfoFactory
		.createParamInfo("metric", Metric.class)
		.setDescription("Method to calculate calc or distance.")
		.setHasDefaultValue(Metric.LEVENSHTEIN_SIM)
		.build();

	default Metric getMetric() {
		return get(METRIC);
	}

	default T setMetric(String value) {
		return set(METRIC, ParamUtil.searchEnum(METRIC, value));
	}

	default T setMetric(Metric value) {
		return set(METRIC, value);
	}

	enum Metric {

		/**
		 * <code>Levenshtein calc</code>
		 */
		LEVENSHTEIN_SIM,

		/**
		 * <code>Levenshtein distance</code>
		 */
		LEVENSHTEIN,

		/**
		 * <code>Lcs calc</code>
		 */
		LCS_SIM,

		/**
		 * <code>Lcs metric</code>
		 */
		LCS,

		/**
		 * <code>ssk calc</code>
		 */
		SSK,
		/**
		 * <code>cosine calc</code>
		 */
		COSINE
	}
}
