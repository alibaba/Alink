package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.ItemCfRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;

/**
 * Recommend items for item with itemCF model.
 */
public class ItemCfSimilarItemsRecommStreamOp
	extends BaseRecommStreamOp <ItemCfSimilarItemsRecommStreamOp>
	implements BaseSimilarItemsRecommParams <ItemCfSimilarItemsRecommStreamOp> {

	private static final long serialVersionUID = 3041711117673110348L;

	public ItemCfSimilarItemsRecommStreamOp(BatchOperator model) {
		this(model, null);
	}

	public ItemCfSimilarItemsRecommStreamOp(BatchOperator model, Params params) {
		super(model, ItemCfRecommKernel::new, RecommType.SIMILAR_ITEMS, params);
	}
}
