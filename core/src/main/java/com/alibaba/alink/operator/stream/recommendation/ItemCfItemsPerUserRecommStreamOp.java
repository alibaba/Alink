package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.ItemCfRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

/**
 * Recommend items for user with itemCF model.
 */
public class ItemCfItemsPerUserRecommStreamOp
	extends BaseRecommStreamOp <ItemCfItemsPerUserRecommStreamOp>
	implements BaseItemsPerUserRecommParams <ItemCfItemsPerUserRecommStreamOp> {

	private static final long serialVersionUID = -9021498920170224399L;

	public ItemCfItemsPerUserRecommStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public ItemCfItemsPerUserRecommStreamOp(BatchOperator <?> model, Params params) {
		super(model, ItemCfRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
