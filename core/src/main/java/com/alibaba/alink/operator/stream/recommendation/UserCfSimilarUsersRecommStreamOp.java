package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.UserCfRecommKernel;
import com.alibaba.alink.params.recommendation.BaseSimilarUsersRecommParams;

/**
 * Recommend users for user with userCF model.
 */
public class UserCfSimilarUsersRecommStreamOp
	extends BaseRecommStreamOp <UserCfSimilarUsersRecommStreamOp>
	implements BaseSimilarUsersRecommParams <UserCfSimilarUsersRecommStreamOp> {

	private static final long serialVersionUID = 8004151126780866021L;

	public UserCfSimilarUsersRecommStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public UserCfSimilarUsersRecommStreamOp(BatchOperator <?> model, Params params) {
		super(model, UserCfRecommKernel::new, RecommType.SIMILAR_USERS, params);
	}
}
