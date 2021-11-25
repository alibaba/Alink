package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.operator.common.similarity.dataConverter.MinHashModelDataConverter;
import com.alibaba.alink.operator.common.similarity.dataConverter.NearestNeighborDataConverter;
import com.alibaba.alink.operator.common.similarity.dataConverter.SimHashModelDataConverter;
import com.alibaba.alink.params.ParamUtil;

/**
 * Params for ApproxStringNearestNeighborIndex.
 */
public interface StringTextApproxNearestNeighborTrainParams<T> extends
	NearestNeighborTrainParams <T>,
	StringTextApproxParams <T> {

	/**
	 * @cn-name 距离类型
	 * @cn 用于计算的距离类型
	 */
	ParamInfo <Metric> METRIC = ParamInfoFactory
		.createParamInfo("metric", Metric.class)
		.setDescription("Method to calculate calc or distance.")
		.setHasDefaultValue(Metric.SIMHASH_HAMMING_SIM)
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

	enum Metric {
		/**
		 * <code>simhash hamming calc;</code>
		 */
		SIMHASH_HAMMING_SIM(new SimHashModelDataConverter()),

		/**
		 * <code>simhash hamming calc;</code>
		 */
		SIMHASH_HAMMING(new SimHashModelDataConverter()),

		/**
		 * <code>minhash calc;</code>
		 */
		MINHASH_JACCARD_SIM(new MinHashModelDataConverter()),
		/**
		 * <code>Jaccard calc;</code>
		 */
		JACCARD_SIM(new MinHashModelDataConverter());

		private NearestNeighborDataConverter dataConverter;

		Metric(NearestNeighborDataConverter dataConverter) {
			this.dataConverter = dataConverter;
		}

		public NearestNeighborDataConverter getDataConverter() {
			return dataConverter;
		}
	}
}
