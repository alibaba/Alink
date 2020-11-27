package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasMetric<T> extends WithParams <T> {

	ParamInfo <Metric> METRIC = ParamInfoFactory
		.createParamInfo("metric", Metric.class)
		.setDescription("Method to calculate calc or distance.")
		.setHasDefaultValue(Metric.LEVENSHTEIN_SIM)
		.setAlias(new String[] {"method", "similarityMethod", "distanceType"})
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

	public enum Metric {
		/**
		 * <code>Levenshtein metric;</code>
		 */
		LEVENSHTEIN,
		/**
		 * <code>Levenshtein calc;</code>
		 */
		LEVENSHTEIN_SIM,
		/**
		 * <code>Lcs metric;</code>
		 */
		LCS,
		/**
		 * <code>Lcs calc;</code>
		 */
		LCS_SIM,
		/**
		 * <code>ssk calc;</code>
		 */
		SSK,
		/**
		 * <code>cosine calc;</code>
		 */
		COSINE,
		/**
		 * <code>simhash hamming metric;</code>
		 */
		SIMHASH_HAMMING,
		/**
		 * <code>simhash hamming calc;</code>
		 */
		SIMHASH_HAMMING_SIM,
		/**
		 * <code>Jaccard calc;</code>
		 */
		JACCARD_SIM
	}
}
