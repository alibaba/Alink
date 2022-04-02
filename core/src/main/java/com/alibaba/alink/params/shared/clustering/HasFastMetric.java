package com.alibaba.alink.params.shared.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.distance.CosineDistance;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.InnerProduct;
import com.alibaba.alink.operator.common.distance.JaccardDistance;
import com.alibaba.alink.operator.common.distance.ManHattanDistance;
import com.alibaba.alink.operator.common.distance.PearsonDistance;
import com.alibaba.alink.params.ParamUtil;

import java.io.Serializable;

/**
 * Params: Distance type for calculating the distance of vector.
 */
public interface HasFastMetric<T> extends WithParams <T> {
	@NameCn("距离度量方式")
	@DescCn("聚类使用的距离类型")
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
