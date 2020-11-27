package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.UserCfRecommKernel;
import com.alibaba.alink.params.recommendation.BaseSimilarUsersRecommParams;

/**
 * Recommend similar users for given user with userCF model.
 */
public class UserCfSimilarUsersRecommender
	extends BaseRecommender <UserCfSimilarUsersRecommender>
	implements BaseSimilarUsersRecommParams <UserCfSimilarUsersRecommender> {

	private static final long serialVersionUID = 6553846224886074049L;

	public UserCfSimilarUsersRecommender() {
		this(null);
	}

	public UserCfSimilarUsersRecommender(Params params) {
		super(UserCfRecommKernel::new, RecommType.SIMILAR_USERS, params);
	}
}
