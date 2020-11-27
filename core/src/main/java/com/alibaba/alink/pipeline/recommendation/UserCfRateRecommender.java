package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.UserCfRecommKernel;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;

/**
 * Rating for user-item pair with userCF model.
 */
public class UserCfRateRecommender
	extends BaseRecommender <UserCfRateRecommender>
	implements BaseRateRecommParams <UserCfRateRecommender> {

	private static final long serialVersionUID = -1839110884922078982L;

	public UserCfRateRecommender() {
		this(null);
	}

	public UserCfRateRecommender(Params params) {
		super(UserCfRecommKernel::new, RecommType.RATE, params);
	}
}
