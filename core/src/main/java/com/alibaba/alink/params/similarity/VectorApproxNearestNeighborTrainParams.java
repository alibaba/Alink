package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.JaccardDistance;
import com.alibaba.alink.operator.common.similarity.Solver;
import com.alibaba.alink.params.ParamUtil;

import java.io.Serializable;

/**
 * Params for ApproxStringNearestNeighborIndex.
 */
public interface VectorApproxNearestNeighborTrainParams<T> extends
	NearestNeighborTrainParams <T>,
	VectorLSHParams <T> {
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
		 * JACCARD
		 */
		JACCARD(new JaccardDistance());

		public FastDistance getFastDistance() {
			return fastDistance;
		}

		private FastDistance fastDistance;

		Metric(FastDistance fastDistance) {
			this.fastDistance = fastDistance;
		}

	}

	ParamInfo <Solver> SOLVER = ParamInfoFactory
		.createParamInfo("solver", Solver.class)
		.setDescription("Method to calc approx topN.")
		.setHasDefaultValue(Solver.KDTREE)
		.build();

	default Solver getSolver() {
		return get(SOLVER);
	}

	default T setSolver(Solver value) {
		return set(SOLVER, value);
	}

	default T setSolver(String value) {
		return set(SOLVER, ParamUtil.searchEnum(SOLVER, value));
	}

}
