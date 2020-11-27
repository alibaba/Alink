package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.UserCfRecommKernel;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

/**
 * Recommend users for item with userCF model.
 */
public class UserCfUsersPerItemRecommStreamOp
	extends BaseRecommStreamOp <UserCfUsersPerItemRecommStreamOp>
	implements BaseUsersPerItemRecommParams <UserCfUsersPerItemRecommStreamOp> {

	private static final long serialVersionUID = -9193410181418869523L;

	public UserCfUsersPerItemRecommStreamOp(BatchOperator model) {
		this(model, null);
	}

	public UserCfUsersPerItemRecommStreamOp(BatchOperator model, Params params) {
		super(model, UserCfRecommKernel::new, RecommType.USERS_PER_ITEM, params);
	}
}
