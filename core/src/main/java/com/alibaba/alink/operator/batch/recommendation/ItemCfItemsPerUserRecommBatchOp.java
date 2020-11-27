package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.ItemCfRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

/**
 * Recommend similar items for the given item.
 */
public class ItemCfItemsPerUserRecommBatchOp extends BaseRecommBatchOp <ItemCfItemsPerUserRecommBatchOp>
	implements BaseItemsPerUserRecommParams <ItemCfItemsPerUserRecommBatchOp> {

	private static final long serialVersionUID = 9103146045129767396L;

	public ItemCfItemsPerUserRecommBatchOp() {
		this(null);
	}

	public ItemCfItemsPerUserRecommBatchOp(Params params) {
		super(ItemCfRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
