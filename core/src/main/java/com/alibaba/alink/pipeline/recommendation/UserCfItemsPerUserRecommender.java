package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.UserCfRecommKernel;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

/**
 * Recommend items for user with userCF model.
 */
@NameCn("UserCf：ItemsPerUser推荐")
public class UserCfItemsPerUserRecommender
	extends BaseRecommender <UserCfItemsPerUserRecommender>
	implements BaseItemsPerUserRecommParams <UserCfItemsPerUserRecommender> {

	private static final long serialVersionUID = -2468725093927188731L;

	public UserCfItemsPerUserRecommender() {
		this(null);
	}

	public UserCfItemsPerUserRecommender(Params params) {
		super(UserCfRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
