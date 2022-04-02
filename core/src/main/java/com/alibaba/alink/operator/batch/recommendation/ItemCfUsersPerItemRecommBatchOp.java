package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.ItemCfRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

/**
 * Recommend similar users for the given item.
 */
@NameCn("ItemCf：UsersPerItem推荐")
public class ItemCfUsersPerItemRecommBatchOp extends BaseRecommBatchOp <ItemCfUsersPerItemRecommBatchOp>
	implements BaseUsersPerItemRecommParams <ItemCfUsersPerItemRecommBatchOp> {

	private static final long serialVersionUID = -6234713805853166771L;

	public ItemCfUsersPerItemRecommBatchOp() {
		this(null);
	}

	public ItemCfUsersPerItemRecommBatchOp(Params params) {
		super(ItemCfRecommKernel::new, RecommType.USERS_PER_ITEM, params);
	}
}
