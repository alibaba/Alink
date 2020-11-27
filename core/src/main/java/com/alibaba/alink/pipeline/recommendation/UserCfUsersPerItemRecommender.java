package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.UserCfRecommKernel;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

/**
 * Recommend user for item with userCF model.
 */
public class UserCfUsersPerItemRecommender
	extends BaseRecommender <UserCfUsersPerItemRecommender>
	implements BaseUsersPerItemRecommParams <UserCfUsersPerItemRecommender> {

	private static final long serialVersionUID = -4547985151986313324L;

	public UserCfUsersPerItemRecommender() {
		this(null);
	}

	public UserCfUsersPerItemRecommender(Params params) {
		super(UserCfRecommKernel::new, RecommType.USERS_PER_ITEM, params);
	}
}
