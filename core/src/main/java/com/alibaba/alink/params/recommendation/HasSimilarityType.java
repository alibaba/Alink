package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.common.distance.CosineDistance;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.JaccardDistance;
import com.alibaba.alink.operator.common.distance.PearsonDistance;
import com.alibaba.alink.params.ParamUtil;

import java.io.Serializable;

/**
 * Params: Distance type for clustering, support EUCLIDEAN and COSINE.
 */
public interface HasSimilarityType<T> extends WithParams <T> {
	ParamInfo <SimilarityType> SIMILARITY_TYPE = ParamInfoFactory
		.createParamInfo("similarityType", SimilarityType.class)
		.setDescription("similarity type for calculationg neighbor")
		.setHasDefaultValue(SimilarityType.COSINE)
		.setAlias(new String[] {"metric"})
		.build();

	default SimilarityType getSimilarityType() {return get(SIMILARITY_TYPE);}

	default T setSimilarityType(SimilarityType value) {return set(SIMILARITY_TYPE, value);}

	default T setSimilarityType(String value) {
		return set(SIMILARITY_TYPE, ParamUtil.searchEnum(SIMILARITY_TYPE, value));
	}

	/**
	 * Various distance types.
	 */
	enum SimilarityType implements Serializable {
		/**
		 * COSINE
		 */
		COSINE(new CosineDistance()),

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

		SimilarityType(FastDistance fastDistance) {
			this.fastDistance = fastDistance;
		}

	}
}
