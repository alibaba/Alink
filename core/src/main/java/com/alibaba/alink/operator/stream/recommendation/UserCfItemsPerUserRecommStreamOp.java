package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.UserCfRecommKernel;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;

/**
 * Recommend items for user with userCF model.
 */
@NameCn("UserCf：ItemsPerUser推荐")
public class UserCfItemsPerUserRecommStreamOp
	extends BaseRecommStreamOp <UserCfItemsPerUserRecommStreamOp>
	implements BaseItemsPerUserRecommParams <UserCfItemsPerUserRecommStreamOp> {

	private static final long serialVersionUID = -4183435377862579691L;

	public UserCfItemsPerUserRecommStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public UserCfItemsPerUserRecommStreamOp(BatchOperator <?> model, Params params) {
		super(model, UserCfRecommKernel::new, RecommType.ITEMS_PER_USER, params);
	}
}
