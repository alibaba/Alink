package com.alibaba.alink.params.shared.clustering;

import com.alibaba.alink.operator.common.distance.*;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

import java.io.Serializable;

/**
 * Params: Distance type for calculating the distance of vector.
 */
public interface HasFastMetric<T> extends WithParams <T> {
	ParamInfo <Metric> METRIC = ParamInfoFactory
		.createParamInfo("metric", Metric.class)
		.setDescription("Distance type for clustering")
		.setHasDefaultValue(Metric.EUCLIDEAN)
		.build();

	default Metric getMetric() {return get(METRIC);}

	default T setMetric(Metric value) {return set(METRIC, value);}

	default T setMetric(String value) {
		return set(METRIC, ParamUtil.searchEnum(METRIC, value));
	}

	/**
	 * Various distance types.
	 */
	enum Metric implements Serializable {
		/**
		 * EUCLIDEAN
		 */
		EUCLIDEAN(new EuclideanDistance()),
		/**
		 * COSINE
		 */
		COSINE(new CosineDistance()),
		/**
		 * INNERPRODUCT
		 */
		INNERPRODUCT(new InnerProduct()),
		/**
		 * CITYBLOCK
		 */
		CITYBLOCK(new ManHattanDistance()),

		/**
		 * JACCARD
		 */
		JACCARD(new JaccardDistance()),

		/**
		 * PEARSON
		 */
		PEARSON(new PearsonDistance());

		public FastDistance getFastDistance() {
			return fastDistance;
		}

		private FastDistance fastDistance;

		Metric(FastDistance fastDistance) {
			this.fastDistance = fastDistance;
		}
	}
}
