package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.ItemCfRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;

/**
 * Recommend similar items for the given item.
 */
public class ItemCfSimilarItemsRecommBatchOp extends BaseRecommBatchOp <ItemCfSimilarItemsRecommBatchOp>
	implements BaseSimilarItemsRecommParams <ItemCfSimilarItemsRecommBatchOp> {

	private static final long serialVersionUID = 6634328547891381236L;

	public ItemCfSimilarItemsRecommBatchOp() {
		this(null);
	}

	public ItemCfSimilarItemsRecommBatchOp(Params params) {
		super(ItemCfRecommKernel::new, RecommType.SIMILAR_ITEMS, params);
	}
}
