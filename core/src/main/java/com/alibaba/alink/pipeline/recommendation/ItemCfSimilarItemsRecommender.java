package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.ItemCfRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;

/**
 * Recommend similar items for given item with itemCF model.
 */
@NameCn("ItemCf：相似items推荐")
public class ItemCfSimilarItemsRecommender
	extends BaseRecommender <ItemCfSimilarItemsRecommender>
	implements BaseSimilarItemsRecommParams <ItemCfSimilarItemsRecommender> {

	private static final long serialVersionUID = -6948797131360742128L;

	public ItemCfSimilarItemsRecommender() {
		this(null);
	}

	public ItemCfSimilarItemsRecommender(Params params) {
		super(ItemCfRecommKernel::new, RecommType.SIMILAR_ITEMS, params);
	}
}
