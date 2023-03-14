package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.common.recommendation.UserCfRecommKernel;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;

/**
 * Rating user-item pair with userCF model.
 */
@NameCn("UserCf：打分推荐")
@NameEn("UserCf：Rate Recomm")
public class UserCfRateRecommStreamOp
	extends BaseRecommStreamOp <UserCfRateRecommStreamOp>
	implements BaseRateRecommParams <UserCfRateRecommStreamOp> {

	private static final long serialVersionUID = 7518888112460284808L;

	public UserCfRateRecommStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public UserCfRateRecommStreamOp(BatchOperator <?> model, Params params) {
		super(model, UserCfRecommKernel::new, RecommType.RATE, params);
	}
}
