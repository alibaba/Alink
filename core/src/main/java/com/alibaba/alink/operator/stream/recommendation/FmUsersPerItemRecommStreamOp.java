package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.FmRecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

/**
 * Fm recommendation batch op for recommending users to item in stream format.
 */
@NameCn("FM：UsersPerItem推荐")
@NameEn("Factorization Machine users per-item recommendation")
public class FmUsersPerItemRecommStreamOp
	extends BaseRecommStreamOp <FmUsersPerItemRecommStreamOp>
	implements BaseUsersPerItemRecommParams <FmUsersPerItemRecommStreamOp> {

	private static final long serialVersionUID = -5509041523214458242L;

	public FmUsersPerItemRecommStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public FmUsersPerItemRecommStreamOp(BatchOperator <?> model, Params params) {
		super(model, FmRecommKernel::new, RecommType.USERS_PER_ITEM, params);
	}
}
