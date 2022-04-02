package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.UserCfRecommKernel;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;

/**
 * Rating for user-item pair with user CF model.
 */
@NameCn("UserCf：打分推荐")
public class UserCfRateRecommBatchOp
	extends BaseRecommBatchOp <UserCfRateRecommBatchOp>
	implements BaseRateRecommParams <UserCfRateRecommBatchOp> {

	private static final long serialVersionUID = 6913095741234341051L;

	public UserCfRateRecommBatchOp() {
		this(null);
	}

	public UserCfRateRecommBatchOp(Params params) {
		super(UserCfRecommKernel::new, RecommType.RATE, params);
	}
}
