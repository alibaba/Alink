package com.alibaba.alink.params.shared.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.common.distance.CosineDistance;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.HaversineDistance;
import com.alibaba.alink.params.ParamUtil;

import java.io.Serializable;

/**
 * Params: Distance type for clustering, support EUCLIDEAN and COSINE.
 */
public interface HasKMeansWithHaversineDistanceType<T> extends WithParams<T> {

	ParamInfo<DistanceType> DISTANCE_TYPE = ParamInfoFactory
		.createParamInfo("distanceType", DistanceType.class)
		.setDescription("Distance type for clustering")
		.setHasDefaultValue(DistanceType.EUCLIDEAN)
		.setAlias(new String[]{"metric"})
		.build();

	default DistanceType getDistanceType() {return get(DISTANCE_TYPE);}

	default T setDistanceType(DistanceType value) {return set(DISTANCE_TYPE, value);}

	default T setDistanceType(String value){
		return set(DISTANCE_TYPE, ParamUtil.searchEnum(DISTANCE_TYPE, value));
	}

	/**
	 * Various distance types.
	 */
	enum DistanceType implements Serializable {
		/**
		 * EUCLIDEAN
		 */
		EUCLIDEAN(new EuclideanDistance()),
		/**
		 * COSINE
		 */
		COSINE(new CosineDistance()),

		/**
		 * HAVERSINE
		 */
		HAVERSINE(new HaversineDistance());

		public FastDistance getFastDistance() {
			return fastDistance;
		}

		private FastDistance fastDistance;

		DistanceType(FastDistance fastDistance){
			this.fastDistance = fastDistance;
		}


	}
}
