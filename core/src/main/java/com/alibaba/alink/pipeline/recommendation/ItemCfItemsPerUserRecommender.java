package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.ItemCfRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

/**
 * Recommend items for user with itemCF model.
 */
public class ItemCfItemsPerUserRecommender
	extends BaseRecommender <ItemCfItemsPerUserRecommender>
	implements BaseItemsPerUserRecommParams <ItemCfItemsPerUserRecommender> {

	private static final long serialVersionUID = -479183577740169370L;

	public ItemCfItemsPerUserRecommender() {
		this(null);
	}

	public ItemCfItemsPerUserRecommender(Params params) {
		super(ItemCfRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
