package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.AlsRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;

/**
 * Recommend similar items for the given item.
 */
public class AlsSimilarItemsRecommender
	extends BaseRecommender <AlsSimilarItemsRecommender>
	implements BaseSimilarItemsRecommParams <AlsSimilarItemsRecommender> {

	private static final long serialVersionUID = -950826094014887702L;

	public AlsSimilarItemsRecommender() {
		this(null);
	}

	public AlsSimilarItemsRecommender(Params params) {
		super(AlsRecommKernel::new, RecommType.SIMILAR_ITEMS, params);
	}
}
