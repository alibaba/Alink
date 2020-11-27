package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.UserCfRecommKernel;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

/**
 * Recommend similar users for the given item.
 */
public class UserCfUsersPerItemRecommBatchOp
	extends BaseRecommBatchOp <UserCfUsersPerItemRecommBatchOp>
	implements BaseUsersPerItemRecommParams <UserCfUsersPerItemRecommBatchOp> {

	private static final long serialVersionUID = 8072363766855231734L;

	public UserCfUsersPerItemRecommBatchOp() {
		this(null);
	}

	public UserCfUsersPerItemRecommBatchOp(Params params) {
		super(UserCfRecommKernel::new, RecommType.USERS_PER_ITEM, params);
	}
}
