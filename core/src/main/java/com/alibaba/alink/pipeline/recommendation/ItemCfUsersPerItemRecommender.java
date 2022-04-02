package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.ItemCfRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

/**
 * Recommend users for item with itemCF model.
 */
@NameCn("ItemCf：UsersPerItem推荐")
public class ItemCfUsersPerItemRecommender
	extends BaseRecommender <ItemCfUsersPerItemRecommender>
	implements BaseUsersPerItemRecommParams <ItemCfUsersPerItemRecommender> {

	private static final long serialVersionUID = -3013352063864817692L;

	public ItemCfUsersPerItemRecommender() {
		this(null);
	}

	public ItemCfUsersPerItemRecommender(Params params) {
		super(ItemCfRecommKernel::new, RecommType.USERS_PER_ITEM, params);
	}
}
